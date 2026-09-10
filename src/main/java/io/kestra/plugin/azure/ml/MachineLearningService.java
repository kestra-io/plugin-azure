package io.kestra.plugin.azure.ml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.AzureBlobDatastore;
import com.azure.resourcemanager.machinelearning.models.DataVersionBase;
import com.azure.resourcemanager.machinelearning.models.Datastore;
import com.azure.resourcemanager.machinelearning.models.JobBase;
import com.azure.resourcemanager.machinelearning.models.JobOutput;
import com.azure.resourcemanager.machinelearning.models.JobStatus;
import com.azure.resourcemanager.machinelearning.models.ModelVersion;
import com.azure.resourcemanager.machinelearning.models.UriFileJobOutput;
import com.azure.resourcemanager.machinelearning.models.UriFolderJobOutput;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;

/**
 * Shared poll-to-terminal-state, status-mapping, and MLflow-metrics logic used by every job-related Azure Machine
 * Learning task ({@link SubmitCommandJob}, {@link SubmitPipelineJob}, {@link CancelJob}, {@link GetJob}).
 */
final class MachineLearningService {
    private static final Pattern DATASTORE_URI = Pattern.compile("^azureml://datastores/([^/]+)/paths/(.+)$");

    private MachineLearningService() {
    }

    /**
     * {@link JobStatus} values are PascalCase strings straight off the ARM REST API (e.g. {@code "CancelRequested"}),
     * not {@link JobState}'s SCREAMING_SNAKE_CASE names, so this maps them explicitly rather than relying on a
     * case transform that would silently break if Azure changes casing.
     */
    static JobState toJobState(JobStatus status) {
        if (status == null) {
            return JobState.UNKNOWN;
        }
        if (status.equals(JobStatus.NOT_STARTED)) {
            return JobState.NOT_STARTED;
        }
        if (status.equals(JobStatus.STARTING)) {
            return JobState.STARTING;
        }
        if (status.equals(JobStatus.PROVISIONING)) {
            return JobState.PROVISIONING;
        }
        if (status.equals(JobStatus.PREPARING)) {
            return JobState.PREPARING;
        }
        if (status.equals(JobStatus.QUEUED)) {
            return JobState.QUEUED;
        }
        if (status.equals(JobStatus.RUNNING)) {
            return JobState.RUNNING;
        }
        if (status.equals(JobStatus.FINALIZING)) {
            return JobState.FINALIZING;
        }
        if (status.equals(JobStatus.CANCEL_REQUESTED)) {
            return JobState.CANCEL_REQUESTED;
        }
        if (status.equals(JobStatus.COMPLETED)) {
            return JobState.COMPLETED;
        }
        if (status.equals(JobStatus.FAILED)) {
            return JobState.FAILED;
        }
        if (status.equals(JobStatus.CANCELED)) {
            return JobState.CANCELED;
        }
        if (status.equals(JobStatus.NOT_RESPONDING)) {
            return JobState.NOT_RESPONDING;
        }
        if (status.equals(JobStatus.PAUSED)) {
            return JobState.PAUSED;
        }
        return JobState.UNKNOWN;
    }

    /**
     * Polls {@code fetch} until the job reaches a terminal state, returning the last fetched job.
     *
     * @throws TimeoutException if {@code maxDuration} elapses before a terminal state is reached
     */
    static JobBase awaitTerminalState(Supplier<JobBase> fetch, Duration interval, Duration maxDuration) throws TimeoutException {
        AtomicReference<JobBase> last = new AtomicReference<>();
        Await.until(
            () ->
            {
                JobBase job = fetch.get();
                last.set(job);
                return toJobState(job.properties().status()).isTerminal();
            },
            interval,
            maxDuration
        );
        return last.get();
    }

    /**
     * Waits for a submitted job to reach a terminal state, cancelling it first when {@code cancelOnTimeout} is set
     * and the wait times out — this is the safeguard against runaway GPU-compute cost that motivated this task
     * group; do not remove it.
     */
    static JobBase awaitCompletion(
        RunContext runContext,
        MachineLearningManager manager,
        String resourceGroupName,
        String workspaceName,
        String jobName,
        Duration interval,
        Duration maxDuration,
        boolean cancelOnTimeout) {
        try {
            return awaitTerminalState(() -> manager.jobs().get(resourceGroupName, workspaceName, jobName), interval, maxDuration);
        } catch (TimeoutException e) {
            if (cancelOnTimeout) {
                try {
                    manager.jobs().cancel(resourceGroupName, workspaceName, jobName);
                } catch (ManagementException cancelException) {
                    runContext.logger().warn("Could not cancel job '{}' after timeout: {}", jobName, cancelException.getMessage());
                }
                throw new IllegalStateException("Job '%s' did not reach a terminal state within %s and was cancelled".formatted(jobName, maxDuration));
            }
            throw new IllegalStateException("Job '%s' did not reach a terminal state within %s; it is still running in Azure Machine Learning".formatted(jobName, maxDuration));
        }
    }

    /**
     * Cancels a job and logs the outcome, swallowing the ARM error when the job is already in a terminal state
     * (kill/cancel is a teardown path and must never fail the task that triggers it).
     */
    static void cancelQuietly(RunContext runContext, MachineLearningManager manager, String resourceGroupName, String workspaceName, String jobName) {
        try {
            manager.jobs().cancel(resourceGroupName, workspaceName, jobName);
            runContext.logger().info("Cancelled Azure Machine Learning job '{}'", jobName);
        } catch (ManagementException e) {
            runContext.logger().warn("Could not cancel job '{}' (it may already be in a terminal state): {}", jobName, e.getMessage());
        }
    }

    static Map<String, URI> namedOutputs(Map<String, JobOutput> outputs) {
        Map<String, URI> result = new HashMap<>();
        if (outputs == null) {
            return result;
        }
        outputs.forEach((name, output) ->
        {
            String uri = switch (output) {
                case UriFileJobOutput uriFileJobOutput -> uriFileJobOutput.uri();
                case UriFolderJobOutput uriFolderJobOutput -> uriFolderJobOutput.uri();
                case null, default -> null;
            };
            if (uri != null) {
                result.put(name, URI.create(uri));
            }
        });
        return result;
    }

    /**
     * Job metrics are logged through MLflow, not exposed by the ARM control-plane SDK: reads the workspace's MLflow
     * tracking URI and calls the MLflow REST API directly, reusing the same AAD bearer token. Best-effort: any
     * failure (missing scope, unreachable endpoint) is logged and yields an empty map rather than failing the task.
     */
    @SuppressWarnings("unchecked")
    static Map<String, Double> mlflowMetrics(RunContext runContext, TokenCredential credential, String mlflowTrackingUri, String jobName) {
        if (mlflowTrackingUri == null || mlflowTrackingUri.isBlank()) {
            return Map.of();
        }

        try {
            String baseUrl = mlflowTrackingUri.replaceFirst("^azureml://", "https://");
            String token = credential.getToken(new TokenRequestContext().addScopes("https://ml.azure.com/.default"))
                .block()
                .getToken();

            URI uri = URI.create(baseUrl + "/api/2.0/mlflow/runs/get?run_id=" + jobName);
            HttpRequest request = HttpRequest.of(uri, Map.of("Authorization", List.of("Bearer " + token)));

            try (HttpClient httpClient = HttpClient.builder().runContext(runContext).build()) {
                Map<String, Object> body = httpClient.request(request, Map.class).getBody();
                if (body == null) {
                    return Map.of();
                }

                var run = (Map<String, Object>) body.get("run");
                var data = run != null ? (Map<String, Object>) run.get("data") : null;
                var metrics = data != null ? (List<Map<String, Object>>) data.get("metrics") : null;
                if (metrics == null) {
                    return Map.of();
                }

                Map<String, Double> result = new HashMap<>();
                for (Map<String, Object> metric : metrics) {
                    Object key = metric.get("key");
                    Object value = metric.get("value");
                    if (key != null && value != null) {
                        result.put(key.toString(), Double.parseDouble(value.toString()));
                    }
                }
                return result;
            }
        } catch (Exception e) {
            runContext.logger().warn("Unable to fetch MLflow metrics for job '{}': {}", jobName, e.getMessage());
            return Map.of();
        }
    }

    /**
     * Azure Machine Learning version identifiers are strings but, unless a caller supplies a custom scheme, they
     * are Azure-assigned monotonically increasing integers; sorting numerically resolves "latest" correctly for
     * that default scheme, falling back to lexicographic order for custom string versions.
     */
    static long versionOrdinal(String version) {
        try {
            return Long.parseLong(version);
        } catch (NumberFormatException e) {
            return Long.MIN_VALUE;
        }
    }

    static ModelVersion latestModelVersion(MachineLearningManager manager, String resourceGroupName, String workspaceName, String modelName) {
        return manager.modelVersions().list(resourceGroupName, workspaceName, modelName).stream()
            .max(Comparator.comparing(v -> versionOrdinal(v.name())))
            .orElse(null);
    }

    static DataVersionBase latestDataVersion(MachineLearningManager manager, String resourceGroupName, String workspaceName, String dataName) {
        return manager.dataVersions().list(resourceGroupName, workspaceName, dataName).stream()
            .max(Comparator.comparing(v -> versionOrdinal(v.name())))
            .orElse(null);
    }

    record DatastorePath(String datastoreName, String path) {
    }

    static DatastorePath parseDatastoreUri(String uri) {
        Matcher matcher = DATASTORE_URI.matcher(uri);
        if (!matcher.matches()) {
            return null;
        }
        return new DatastorePath(matcher.group(1), matcher.group(2));
    }

    static BlobContainerClient blobContainerClient(TokenCredential credential, String accountName, String containerName) {
        return new BlobContainerClientBuilder()
            .endpoint("https://%s.blob.core.windows.net".formatted(accountName))
            .containerName(containerName)
            .credential(credential)
            .buildClient();
    }

    static Datastore defaultDatastore(MachineLearningManager manager, String resourceGroupName, String workspaceName) {
        return manager.datastores().list(resourceGroupName, workspaceName).stream()
            .filter(datastore -> Boolean.TRUE.equals(datastore.properties().isDefault()))
            .findFirst()
            .orElseThrow(
                () -> new IllegalStateException(
                    "No default datastore found in workspace '%s' — register one, or provide a `datastoreUri`/job output source instead of internal storage".formatted(workspaceName)
                )
            );
    }

    /**
     * Uploads a Kestra internal-storage file to the workspace's default datastore, since the ARM control-plane SDK
     * only registers asset metadata and never moves bytes itself. Returns the resulting {@code azureml://} path so
     * it can be used directly as a model/data asset URI.
     */
    static String uploadToDefaultDatastore(
        RunContext runContext,
        MachineLearningManager manager,
        TokenCredential credential,
        String resourceGroupName,
        String workspaceName,
        URI internalStorageUri,
        String destinationPath) throws IOException {
        Datastore datastore = defaultDatastore(manager, resourceGroupName, workspaceName);
        if (!(datastore.properties() instanceof AzureBlobDatastore blobDatastore)) {
            throw new IllegalStateException(
                "Default datastore '%s' in workspace '%s' is not an Azure Blob datastore, uploading from Kestra internal storage is not supported for this datastore type"
                    .formatted(datastore.name(), workspaceName)
            );
        }

        BlobContainerClient container = blobContainerClient(credential, blobDatastore.accountName(), blobDatastore.containerName());
        try (InputStream inputStream = runContext.storage().getFile(internalStorageUri)) {
            container.getBlobClient(destinationPath).upload(inputStream, true);
        }

        return "azureml://datastores/%s/paths/%s".formatted(datastore.name(), destinationPath);
    }
}
