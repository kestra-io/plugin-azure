package io.kestra.plugin.azure.batch;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;

public class BatchService {
    public static BatchClient client(String endpoint, String account, String accessKey, RunContext runContext) throws IllegalVariableEvaluationException {
        return BatchClient.open(new BatchSharedKeyCredentials(
            runContext.render(endpoint),
            runContext.render(account),
            runContext.render(accessKey)
        ));
    }
}
