package io.kestra.plugin.azure.eventhubs;

import java.net.URL;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.repositories.ExecutionRepositoryInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.azure.AbstractFlociTest;
import io.kestra.plugin.azure.eventhubs.serdes.Serdes;

import io.micronaut.context.annotation.Value;
import io.micronaut.data.model.Pageable;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true, startScheduler = true)
class RealtimeTriggerTest extends AbstractFlociTest {

    @Inject
    private ExecutionRepositoryInterface executionRepository;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Value("${kestra.variables.globals.azure.eventhubs.connection-string}")
    protected String connectionString;

    @Value("${kestra.variables.globals.azure.eventhubs.eventhub-name}")
    protected String eventHubName;

    @BeforeEach
    void useFlociEventHubs() {
        this.connectionString = eventHubConnectionString();
        this.eventHubName = EVENT_HUB;
    }

    @Test
    void testTrigger() throws Exception {
        repositoryLoader.load(flow());

        produceEvents();

        long deadline = System.nanoTime() + Duration.ofMinutes(1).toNanos();
        Object body = null;
        while (System.nanoTime() < deadline && body == null) {
            for (Execution execution : executionRepository.findByFlowId(
                TenantService.MAIN_TENANT,
                "io.kestra.tests",
                "eventhubs-realtime-listen",
                Pageable.from(1, 20)
            )) {
                if (execution.getState().isSuccess() && execution.getTrigger() != null) {
                    Object candidate = execution.getTrigger().getVariables().get("body");
                    if ("event-1".equals(candidate)) {
                        body = candidate;
                        break;
                    }
                }
            }
            if (body == null) {
                Thread.sleep(500);
            }
        }
        assertThat(body, is("event-1"));
    }

    private void produceEvents() throws Exception {
        Produce task = Produce.builder()
            .id(ConsumeTest.class.getSimpleName())
            .type(Produce.class.getName())
            .bodySerializer(Property.ofValue(Serdes.STRING))
            .eventHubName(Property.ofValue(eventHubName))
            .namespace(Property.ofValue(EVENT_HUB_NAMESPACE))
            .connectionString(Property.ofValue(connectionString))
            .from(
                List.of(
                    ImmutableMap.builder()
                        .put("body", "event-1")
                        .build()
                )
            )
            .build();
        task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }

    private URL flow() throws java.io.IOException {
        String yaml = """
            id: eventhubs-realtime-listen
            namespace: io.kestra.tests

            triggers:
              - id: watch
                type: %s
                eventHubName: "%s"
                namespace: "%s"
                connectionString: "%s"
                bodyDeserializer: STRING
                consumerGroup: "$Default"
                checkpointStoreProperties:
                  containerName: "%s"
                  connectionString: "%s"

            tasks:
              - id: end
                type: io.kestra.plugin.core.debug.Return
                format: "{{task.id}} > {{taskrun.startDate}}"
            """.formatted(
            RealtimeTrigger.class.getName(),
            EVENT_HUB,
            EVENT_HUB_NAMESPACE,
            eventHubConnectionString(),
            CHECKPOINT_CONTAINER,
            storageConnectionString()
        );
        var path = Files.createTempFile("eventhubs-realtime-listen", ".yaml");
        path.toFile().deleteOnExit();
        return Files.writeString(path, yaml).toUri().toURL();
    }
}
