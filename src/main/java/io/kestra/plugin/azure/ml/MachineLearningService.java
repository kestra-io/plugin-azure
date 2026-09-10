package io.kestra.plugin.azure.ml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import com.azure.resourcemanager.machinelearning.models.CommandJob;
import com.azure.resourcemanager.machinelearning.models.DataContainer;
import com.azure.resourcemanager.machinelearning.models.DataVersionBase;
import com.azure.resourcemanager.machinelearning.models.Datastore;
import com.azure.resourcemanager.machinelearning.models.JobBase;
import com.azure.resourcemanager.machinelearning.models.JobOutput;
import com.azure.resourcemanager.machinelearning.models.JobStatus;
import com.azure.resourcemanager.machinelearning.models.ModelContainer;
import com.azure.resourcemanager.machinelearning.models.ModelVersion;
import com.azure.resourcemanager.machinelearning.models.PipelineJob;
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

    /**
     * Backs every bounded/off-thread ARM call in this package (a submit call's create() timeout, ScaleCluster's
     * autoscale update, CancellableJob's async cancel dispatch) with dedicated daemon threads, instead of each one
     * separately competing for the JVM-wide {@link java.util.concurrent.ForkJoinPool#commonPool()}, which is also
     * used by unrelated code throughout the JVM and can be starved by several concurrent, minutes-long blocking
     * calls.
     */
    static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "azure-ml-async");
        thread.setDaemon(true);
        return thread;
    });

    private MachineLearningService() {
    }

    /**
     * Bounds a blocking ARM call (the SDK exposes no client-side timeout of its own) so a stalled long-running
     * operation fails cleanly instead of hanging the task indefinitely. A {@link RuntimeException} thrown by
     * {@code action} (e.g. {@link ManagementException}) propagates as itself, unwrapped — so a caller's existing
     * {@code catch (ManagementException e)} keeps working transparently, as if this call were still synchronous.
     */
    static <T> T withTimeout(Supplier<T> action, Duration timeout, Supplier<String> timeoutMessage) {
        CompletableFuture<T> future = CompletableFuture.supplyAsync(action, EXECUTOR);
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // Cannot actually stop a synchronous, non-interruptible SDK call underneath — this is best-effort.
            future.cancel(true);
            throw new IllegalStateException(timeoutMessage.get(), e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(cause.getMessage(), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }

    /**
     * Null-safe: a job's {@code properties()} is not guaranteed non-null (a malformed/partial ARM response), unlike
     * a compute resource's, which every other call site already accounts for. Prefer this over
     * {@code toJobState(job.properties().status())} at any job call site.
     */
    static JobState toJobState(JobBase job) {
        return toJobState(job.properties() != null ? job.properties().status() : null);
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
    static JobBase awaitTerminalState(RunContext runContext, Supplier<JobBase> fetch, Duration interval, Duration maxDuration) throws TimeoutException {
        AtomicReference<JobBase> last = new AtomicReference<>();
        // Await.until checks the timeout only between sleeps, not during one — an interval larger than maxDuration
        // would otherwise sleep straight through the whole timeout budget before the first check even happens.
        // Capping the poll interval to maxDuration keeps the timeout bound close to what is actually configured.
        Duration pollInterval = interval.compareTo(maxDuration) > 0 ? maxDuration : interval;
        Await.until(
            () ->
            {
                JobBase job;
                try {
                    job = fetch.get();
                } catch (ManagementException e) {
                    // A single transient ARM error (throttling, a network blip) must not fail a wait that can span
                    // hours — log it and keep polling; a persistent problem still surfaces via the timeout below.
                    runContext.logger().warn("Transient error polling job status, will retry: {}", e.getMessage());
                    return false;
                }
                last.set(job);
                return toJobState(job).isTerminal();
            },
            pollInterval,
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
            return awaitTerminalState(runContext, () -> manager.jobs().get(resourceGroupName, workspaceName, jobName), interval, maxDuration);
        } catch (TimeoutException e) {
            if (cancelOnTimeout) {
                // Goes through the same cancelQuietly() path a kill signal uses, instead of issuing its own raw
                // cancel() call. A kill and this timeout can still both invoke it around the same moment — that is
                // fine, cancelQuietly()'s own retry/error handling tolerates a duplicate or already-in-flight cancel
                // request; this only avoids the two paths diverging in how they call and interpret the ARM API.
                cancelQuietly(runContext, manager, resourceGroupName, workspaceName, jobName);

                // Cancellation is itself asynchronous (a job can sit in CANCEL_REQUESTED for minutes) — confirm it
                // actually reached a terminal state within a short, bounded grace period instead of just asserting
                // it happened; if it hasn't landed yet, say so rather than lying about the job's real state. The
                // poll cadence is capped along with the grace period itself — reusing the caller's (possibly much
                // larger) interval here would mean a single poll could outlast the whole grace window.
                Duration cancelGrace = Duration.ofSeconds(30);
                Duration cancelPollInterval = interval.compareTo(Duration.ofSeconds(5)) < 0 ? interval : Duration.ofSeconds(5);
                try {
                    JobBase cancelled = awaitTerminalState(runContext, () -> manager.jobs().get(resourceGroupName, workspaceName, jobName), cancelPollInterval, cancelGrace);
                    JobState finalState = toJobState(cancelled);
                    throw new IllegalStateException("Job '%s' did not reach a terminal state within %s; cancellation was requested and confirmed (final status '%s')".formatted(jobName, maxDuration, finalState));
                } catch (TimeoutException confirmTimeout) {
                    throw new IllegalStateException("Job '%s' did not reach a terminal state within %s; cancellation was requested but not yet confirmed — check its status in Azure ML Studio".formatted(jobName, maxDuration));
                }
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
            Await.until(
                () -> {
                    try {
                        manager.jobs().cancel(resourceGroupName, workspaceName, jobName);
                        return true;
                    } catch (ManagementException e) {
                        if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                            // A kill signal can arrive while the job is still being submitted (arm() runs before
                            // the create() call returns) — the job id is known client-side before Azure
                            // acknowledges it server-side, so retry until it shows up rather than giving up too
                            // soon. This runs off-thread (see CancellableJob), so a generous window costs nothing
                            // beyond the worker's own kill-signal timeout — unlike blocking the caller directly.
                            return false;
                        }
                        throw e;
                    }
                },
                Duration.ofSeconds(2),
                Duration.ofMinutes(2)
            );
            runContext.logger().info("Cancelled Azure Machine Learning job '{}'", jobName);
        } catch (TimeoutException timeoutException) {
            runContext.logger().warn("Could not cancel job '{}': it was never found within the retry window — it may not have been created", jobName);
        } catch (ManagementException e) {
            runContext.logger().warn("Could not cancel job '{}' (it may already be in a terminal state): {}", jobName, e.getMessage());
        }
    }

    /**
     * Checked only for an explicitly-supplied {@code name} (an auto-generated UUID essentially never collides).
     * The kill lifecycle is armed against {@code jobName} before {@code create()} is called, so that a kill signal
     * arriving while the request is in flight is not lost — but that means a kill delivered in that same window,
     * before an eventual 409 name-collision is even detected, would otherwise cancel whatever unrelated job already
     * holds that name. Failing fast here, before ever arming, closes that window for the realistic case.
     */
    static void ensureJobNameAvailable(MachineLearningManager manager, String resourceGroupName, String workspaceName, String jobName) {
        try {
            manager.jobs().get(resourceGroupName, workspaceName, jobName);
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                return;
            }
            throw e;
        }
        throw new IllegalArgumentException(
            "Job '%s' already exists in workspace '%s' — Azure ML job names are unique per workspace; set a different `name` or leave it empty to auto-generate one"
                .formatted(jobName, workspaceName)
        );
    }

    /**
     * Called after job submission itself threw, to decide whether the armed cancel action should be cleared.
     * Azure's create() call is a long-running operation: a client-side failure (a timeout polling it to
     * completion, a network blip after the initial request landed) does not guarantee the job was never created
     * server-side. Disarming unconditionally on any exception would risk silently dropping the only cancel path
     * for a job that is, in fact, running (and billing) in Azure — so this confirms non-existence first, and
     * stays conservative (leaves the action armed) whenever that confirmation itself cannot be made.
     */
    static void disarmIfJobDoesNotExist(CancellableJob lifecycle, MachineLearningManager manager, String resourceGroupName, String workspaceName, String jobName) {
        try {
            manager.jobs().get(resourceGroupName, workspaceName, jobName);
            // It exists — leave the cancel action armed; a later kill signal must still be able to reach it.
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                lifecycle.disarm();
            }
            // Any other failure checking: stay conservative and leave the action armed.
        }
    }

    /**
     * A job's declared outputs live on its concrete properties subtype ({@code CommandJob} or {@code PipelineJob}
     * today); this extracts them regardless of which kind of job was submitted, instead of silently returning none
     * for job types other than the caller happened to assume.
     */
    static Map<String, JobOutput> jobOutputs(JobBase job) {
        return switch (job.properties()) {
            case CommandJob commandJob -> commandJob.outputs();
            case PipelineJob pipelineJob -> pipelineJob.outputs();
            case null, default -> null;
        };
    }

    /**
     * A job's {@code computeId} property is not the bare compute name but its full ARM resource ID — the Azure ML
     * REST API rejects a bare name here (unlike, e.g., {@code computes().get(...)}, which does take a bare name).
     */
    static String computeResourceId(String subscriptionId, String resourceGroupName, String workspaceName, String computeName) {
        return "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.MachineLearningServices/workspaces/%s/computes/%s"
            .formatted(subscriptionId, resourceGroupName, workspaceName, computeName);
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
    static Map<String, Double> mlflowMetrics(RunContext runContext, TokenCredential credential, MachineLearningManager manager, String resourceGroupName, String workspaceName, String jobName) {
        try {
            String mlflowTrackingUri = manager.workspaces().getByResourceGroup(resourceGroupName, workspaceName).mlFlowTrackingUri();
            if (mlflowTrackingUri == null || mlflowTrackingUri.isBlank()) {
                return Map.of();
            }

            String baseUrl = mlflowTrackingUri.replaceFirst("^azureml://", "https://");
            String token = credential.getToken(new TokenRequestContext().addScopes("https://ml.azure.com/.default"))
                .block(Duration.ofSeconds(15))
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
            // A blocking call in this method (e.g. the token fetch) wraps a thread interruption as an unchecked
            // exception rather than surfacing InterruptedException directly — restore the interrupt status instead
            // of silently swallowing it along with every other best-effort failure, so a kill signal delivered
            // while this call was blocked is still observable by the caller.
            if (e instanceof InterruptedException || e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            runContext.logger().warn("Unable to fetch MLflow metrics for job '{}': {}", jobName, e.getMessage());
            return Map.of();
        }
    }

    /**
     * "Latest" is resolved by actual creation time ({@code systemData().createdAt()}), not by comparing version
     * identifiers — Azure's default scheme assigns monotonically increasing integers, but a caller may register an
     * explicit non-numeric version at any point, and a version-string comparator has no universally correct way to
     * rank a numeric version against a custom one (whichever side "wins" the comparison, it does so forever,
     * permanently hiding one kind of version from "latest" resolution). Creation time carries no such ambiguity.
     */
    /**
     * A model version's {@code properties()} is not guaranteed non-null, matching the same nullability every other
     * ARM resource in this package already accounts for. A missing storage URI means the version is unusable
     * regardless, so this fails with an actionable message rather than deferring to an NPE at the call site.
     */
    static String requireModelUri(ModelVersion modelVersion, String modelName) {
        String modelUri = modelVersion.properties() != null ? modelVersion.properties().modelUri() : null;
        if (modelUri == null) {
            throw new IllegalStateException("Model '%s' version '%s' has no storage URI recorded — this model version appears to be malformed or incomplete".formatted(modelName, modelVersion.name()));
        }
        return modelUri;
    }

    static String modelType(ModelVersion modelVersion) {
        return modelVersion.properties() != null ? modelVersion.properties().modelType() : null;
    }

    /**
     * Same nullability concern as {@link #requireModelUri}, for data asset versions.
     */
    static String requireDataUri(DataVersionBase dataVersion, String dataName) {
        String dataUri = dataVersion.properties() != null ? dataVersion.properties().dataUri() : null;
        if (dataUri == null) {
            throw new IllegalStateException("Data asset '%s' version '%s' has no storage URI recorded — this version appears to be malformed or incomplete".formatted(dataName, dataVersion.name()));
        }
        return dataUri;
    }

    /**
     * A container's {@code properties()} is not guaranteed non-null (e.g. transiently, right after creation) —
     * fail with an actionable message instead of an NPE when computing the version to auto-increment to.
     */
    static String requireNextVersion(ModelContainer container, String modelName) {
        if (container.properties() == null) {
            throw new IllegalStateException("Could not determine the next version for model '%s' — its container properties are not yet available, retry shortly".formatted(modelName));
        }
        return container.properties().nextVersion();
    }

    /**
     * A 409 on a version {@code create()} call almost always means the version already exists, but not
     * exclusively — this confirms it before blaming a version collision, so an unrelated conflict (e.g. transient
     * container-property propagation) doesn't get mislabeled and mask the real cause.
     */
    static boolean modelVersionExists(MachineLearningManager manager, String resourceGroupName, String workspaceName, String modelName, String version) {
        try {
            manager.modelVersions().get(resourceGroupName, workspaceName, modelName, version);
            return true;
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                return false;
            }
            // Don't guess: a 503/403/throttling response here doesn't tell us whether the version exists, and
            // reporting "already exists" would mask what is actually a fresh, unrelated failure. Let it propagate.
            throw e;
        }
    }

    static boolean dataVersionExists(MachineLearningManager manager, String resourceGroupName, String workspaceName, String dataName, String version) {
        try {
            manager.dataVersions().get(resourceGroupName, workspaceName, dataName, version);
            return true;
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    static String requireNextVersion(DataContainer container, String dataName) {
        if (container.properties() == null) {
            throw new IllegalStateException("Could not determine the next version for data asset '%s' — its container properties are not yet available, retry shortly".formatted(dataName));
        }
        return container.properties().nextVersion();
    }

    static ModelVersion latestModelVersion(MachineLearningManager manager, String resourceGroupName, String workspaceName, String modelName) {
        return manager.modelVersions().list(resourceGroupName, workspaceName, modelName).stream()
            .max(Comparator.comparing((ModelVersion v) -> v.systemData() != null ? v.systemData().createdAt() : null, Comparator.nullsFirst(Comparator.naturalOrder())))
            .orElse(null);
    }

    static DataVersionBase latestDataVersion(MachineLearningManager manager, String resourceGroupName, String workspaceName, String dataName) {
        return manager.dataVersions().list(resourceGroupName, workspaceName, dataName).stream()
            .max(Comparator.comparing((DataVersionBase v) -> v.systemData() != null ? v.systemData().createdAt() : null, Comparator.nullsFirst(Comparator.naturalOrder())))
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
            .filter(datastore -> datastore.properties() != null && Boolean.TRUE.equals(datastore.properties().isDefault()))
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
