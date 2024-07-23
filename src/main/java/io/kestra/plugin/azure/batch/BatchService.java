package io.kestra.plugin.azure.batch;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.DetailLevel;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.models.CloudJob;
import com.microsoft.azure.batch.protocol.models.MetadataItem;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.runners.RunContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BatchService {
    public static BatchClient client(String endpoint, String account, String accessKey, RunContext runContext) throws IllegalVariableEvaluationException {
        return BatchClient.open(new BatchSharedKeyCredentials(
            runContext.render(endpoint),
            runContext.render(account),
            runContext.render(accessKey)
        ));
    }

    public static Optional<CloudJob> getExistingJob(RunContext runContext, BatchClient client, String baseJobName) throws IOException {
        var listJobsResponse = client.jobOperations().listJobs(new DetailLevel.Builder().withFilterClause("startswith(id, '" + baseJobName + "')").build());
        return listJobsResponse.stream().filter(cloudJob -> hasAllLabels(runContext, cloudJob.metadata())).findFirst();
    }

    private static boolean hasAllLabels(RunContext runContext, List<MetadataItem> metadata) {
        Map<String, String> labels = ScriptService.labels(runContext, "kestra-", true, true);
        for (MetadataItem metadataItem : metadata) {
            String value = labels.get(metadataItem.name());
            if (value == null || !value.equals(metadataItem.value())) {
                return false;
            }
        }
        return true;
    }
}
