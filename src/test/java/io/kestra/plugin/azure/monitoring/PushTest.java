package io.kestra.plugin.azure.monitoring;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@KestraTest
class PushTest {
    private static final String ENDPOINT = "https://my-dce-a1b2.westeurope.ingest.monitor.azure.com";
    private static final String DCR_PATH = "/dataCollectionRules/dcr-0123456789abcdef/streams/Custom-MyStream";

    @Inject
    private RunContextFactory runContextFactory;

    private static Push.PushBuilder<?, ?> task() {
        return Push.builder()
            .id(PushTest.class.getSimpleName())
            .type(Push.class.getName())
            .tenantId(Property.ofValue("tenant"))
            .endpoint(Property.ofValue(ENDPOINT))
            .path(Property.ofValue(DCR_PATH))
            .metrics(Property.ofValue(Map.of("TimeGenerated", "2024-01-01T00:00:00Z", "Computer", "worker-01")));
    }

    /** Stubs only the client factory, so the real SDK still builds the request. */
    private static Push sending(Push task, IngestionStub stub) throws Exception {
        Push spied = spy(task);
        doReturn(stub.client(ENDPOINT)).when(spied).ingestionClient(any(RunContext.class));

        return spied;
    }

    @Test
    void shouldUploadTheRecordThroughTheSdk() throws Exception {
        var stub = IngestionStub.respondingWith(204);

        var output = sending(task().build(), stub).run(runContextFactory.of());

        assertThat(output, notNullValue());
        assertThat(stub.requestMethod(), is("POST"));
        assertThat(stub.requestUrl(), startsWith(ENDPOINT + DCR_PATH));
        // the hand-rolled version never sent api-version, which the Logs Ingestion API requires
        assertThat(stub.requestUrl(), containsString("api-version="));

        // the API takes an array of records, the previous implementation posted a bare object
        var body = stub.requestBody();
        assertThat(body, startsWith("["));
        assertThat(body, containsString("\"Computer\":\"worker-01\""));
    }

    @Test
    void shouldGzipThePayload() throws Exception {
        var stub = IngestionStub.respondingWith(204);

        sending(task().build(), stub).run(runContextFactory.of());

        assertThat(stub.gzipped(), is(true));
    }

    @Test
    void shouldAcceptAPathCarryingAnApiVersionQuery() throws Exception {
        var stub = IngestionStub.respondingWith(204);

        sending(task().path(Property.ofValue(DCR_PATH + "?api-version=2023-01-01")).build(), stub)
            .run(runContextFactory.of());

        assertThat(stub.requestUrl(), startsWith(ENDPOINT + DCR_PATH));
    }

    @Test
    void shouldRejectAPathThatIsNotADcrIngestionPath() throws Exception {
        var task = sending(task().path(Property.ofValue("/v1/metrics")).build(), IngestionStub.respondingWith(204));

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("/dataCollectionRules/"));
        assertThat(exception.getMessage(), containsString("/v1/metrics"));
    }

    @Test
    void shouldRejectAnEmptyRecord() throws Exception {
        var task = sending(task().metrics(Property.ofValue(Map.of())).build(), IngestionStub.respondingWith(204));

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("metrics is required"));
    }
}
