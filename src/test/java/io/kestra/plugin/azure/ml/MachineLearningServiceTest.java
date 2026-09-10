package io.kestra.plugin.azure.ml;

import java.net.URI;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.azure.resourcemanager.machinelearning.models.JobStatus;
import com.azure.resourcemanager.machinelearning.models.UriFileJobOutput;
import com.azure.resourcemanager.machinelearning.models.UriFolderJobOutput;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Pure unit tests for the shared status-mapping and URI-parsing logic, which do not require live Azure credentials
 * and therefore run unconditionally in CI, unlike the credential-gated integration tests for the tasks themselves.
 */
class MachineLearningServiceTest {
    @Test
    void toJobStateMapsKnownStatuses() {
        assertThat(MachineLearningService.toJobState(JobStatus.COMPLETED), is(JobState.COMPLETED));
        assertThat(MachineLearningService.toJobState(JobStatus.RUNNING), is(JobState.RUNNING));
        assertThat(MachineLearningService.toJobState(JobStatus.CANCEL_REQUESTED), is(JobState.CANCEL_REQUESTED));
    }

    @Test
    void toJobStateFallsBackToUnknown() {
        assertThat(MachineLearningService.toJobState(null), is(JobState.UNKNOWN));
        assertThat(MachineLearningService.toJobState(JobStatus.fromString("SomeFutureState")), is(JobState.UNKNOWN));
    }

    @Test
    void jobStateTerminalAndFailureFlags() {
        assertThat(JobState.COMPLETED.isTerminal(), is(true));
        assertThat(JobState.FAILED.isTerminal(), is(true));
        assertThat(JobState.CANCELED.isTerminal(), is(true));
        assertThat(JobState.RUNNING.isTerminal(), is(false));

        assertThat(JobState.FAILED.isFailure(), is(true));
        assertThat(JobState.CANCELED.isFailure(), is(true));
        assertThat(JobState.COMPLETED.isFailure(), is(false));
    }

    @Test
    void parseDatastoreUriExtractsNameAndPath() {
        var parsed = MachineLearningService.parseDatastoreUri("azureml://datastores/workspaceblobstore/paths/models/v1/model.pkl");

        assertThat(parsed, is(notNullValue()));
        assertThat(parsed.datastoreName(), is("workspaceblobstore"));
        assertThat(parsed.path(), is("models/v1/model.pkl"));
    }

    @Test
    void parseDatastoreUriReturnsNullForNonMatchingUri() {
        assertThat(MachineLearningService.parseDatastoreUri("https://myaccount.blob.core.windows.net/container/path"), is(nullValue()));
    }

    @Test
    void namedOutputsExtractsUriFileAndFolderOutputs() {
        var outputs = Map.<String, com.azure.resourcemanager.machinelearning.models.JobOutput> of(
            "model_dir", new UriFolderJobOutput().withUri("azureml://datastores/workspaceblobstore/paths/outputs/model"),
            "report", new UriFileJobOutput().withUri("azureml://datastores/workspaceblobstore/paths/outputs/report.json")
        );

        var named = MachineLearningService.namedOutputs(outputs);

        assertThat(named.size(), is(2));
        assertThat(named.get("model_dir"), is(URI.create("azureml://datastores/workspaceblobstore/paths/outputs/model")));
        assertThat(named.get("report"), is(URI.create("azureml://datastores/workspaceblobstore/paths/outputs/report.json")));
    }

    @Test
    void namedOutputsReturnsEmptyMapForNullInput() {
        assertThat(MachineLearningService.namedOutputs(null).isEmpty(), is(true));
    }
}
