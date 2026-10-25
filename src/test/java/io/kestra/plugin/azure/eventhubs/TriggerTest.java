package io.kestra.plugin.azure.eventhubs;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.repositories.ExecutionRepositoryInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.azure.eventhubs.serdes.Serdes;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@KestraTest(startRunner = true, startScheduler = true)
class TriggerTest extends AbstractEventHubsTriggerTest {

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
        repositoryLoader.load(flow("eventhubs-listen", Trigger.class, "interval: PT10S"));

        produceEvents();

        var execution = awaitExecution(executionRepository, "eventhubs-listen", ignored -> true);
        assertThat(((Number) execution.getTrigger().getVariables().get("eventsCount")).intValue(), greaterThanOrEqualTo(2));
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
                        .build(),
                    ImmutableMap.builder()
                        .put("body", "event-2")
                        .build()
                )
            )
            .build();
        task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }
}
