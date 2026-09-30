package io.kestra.plugin.azure.monitoring;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.azure.monitor.ingestion.LogsIngestionClient;

import io.kestra.core.http.HttpResponse;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@KestraTest
class PushTest {
    private static final String ENDPOINT = "https://my-dce-a1b2.westeurope.ingest.monitor.azure.com";
    private static final String RULE_ID = "dcr-0123456789abcdef";
    private static final String STREAM = "Custom-MyStream";
    private static final String DCR_PATH = "/dataCollectionRules/%s/streams/%s".formatted(RULE_ID, STREAM);
    private static final Map<String, Object> RECORD = Map.of("TimeGenerated", "2024-01-01T00:00:00Z", "Computer", "worker-01");

    @Inject
    private RunContextFactory runContextFactory;

    private static Push.PushBuilder<?, ?> task() {
        return Push.builder()
            .id(PushTest.class.getSimpleName())
            .type(Push.class.getName())
            .tenantId(Property.ofValue("tenant"))
            .endpoint(Property.ofValue(ENDPOINT))
            .path(Property.ofValue(DCR_PATH))
            .metrics(Property.ofValue(RECORD));
    }

    /** Stubs both routes, so a test asserts which one the path selected. */
    private static Push sending(Push task, LogsIngestionClient client) throws Exception {
        Push spied = spy(task);
        doReturn(client).when(spied).ingestionClient(any(RunContext.class));
        doReturn(HttpResponse.of(HttpResponse.Status.OK, Map.<String, Object> of("accepted", true)))
            .when(spied).postVerbatim(any(RunContext.class), anyString(), any());

        return spied;
    }

    @Test
    void shouldUploadThroughTheSdk() throws Exception {
        var client = mock(LogsIngestionClient.class);
        var task = sending(task().build(), client);

        var output = task.run(runContextFactory.of());

        // the API takes an array of records, the pre-SDK version posted a bare object
        verify(client).upload(RULE_ID, STREAM, List.of(RECORD));
        verify(task, never()).postVerbatim(any(), anyString(), any());
        assertThat(output.getBody(), nullValue());
    }

    @Test
    void shouldIgnoreAnApiVersionQueryOnThePath() throws Exception {
        var client = mock(LogsIngestionClient.class);

        sending(task().path(Property.ofValue(DCR_PATH + "?api-version=2023-01-01")).build(), client)
            .run(runContextFactory.of());

        verify(client).upload(RULE_ID, STREAM, List.of(RECORD));
    }

    @Test
    void shouldPostVerbatimWhenThePathOnlyContainsADataCollectionRule() throws Exception {
        // an unanchored match would upload to dcr-1/S here and silently drop /foo and /extra
        var wrapped = "/foo" + DCR_PATH + "/extra";
        var client = mock(LogsIngestionClient.class);
        var task = sending(task().path(Property.ofValue(wrapped)).build(), client);

        task.run(runContextFactory.of());

        verify(task).postVerbatim(any(RunContext.class), eq(wrapped), eq(RECORD));
        verifyNoInteractions(client);
    }

    @Test
    void shouldPostVerbatimWhenThePathIsNotADataCollectionRule() throws Exception {
        var legacyPath = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm1/metrics";
        var client = mock(LogsIngestionClient.class);
        var task = sending(task().path(Property.ofValue(legacyPath)).build(), client);

        var output = task.run(runContextFactory.of());

        verify(task).postVerbatim(any(RunContext.class), eq(legacyPath), eq(RECORD));
        verifyNoInteractions(client);
        assertThat(output.getBody(), hasEntry("accepted", true));
    }
}
