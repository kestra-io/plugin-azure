package io.kestra.plugin.azure.eventhubs;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.repositories.ExecutionRepositoryInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.Await;
import io.kestra.plugin.azure.AbstractFlociTest;

import io.micronaut.data.model.Pageable;

// Event Hubs triggers read their settings from flow YAML, so these tests write flows pointing at the emulator.
abstract class AbstractEventHubsTriggerTest extends AbstractFlociTest {
    private static final String NAMESPACE = "io.kestra.tests";

    protected static URL flow(String id, Class<?> triggerType, String extraProperty) throws IOException {
        var yaml = """
            id: %s
            namespace: %s

            triggers:
              - id: watch
                type: %s
                %s
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
            id,
            NAMESPACE,
            triggerType.getName(),
            extraProperty,
            EVENT_HUB,
            EVENT_HUB_NAMESPACE,
            eventHubConnectionString(),
            CHECKPOINT_CONTAINER,
            floci.getStorageConnectionString()
        );
        var path = Files.createTempFile(id, ".yaml");
        path.toFile().deleteOnExit();
        return Files.writeString(path, yaml).toUri().toURL();
    }

    protected static Execution awaitExecution(ExecutionRepositoryInterface executionRepository, String flowId, Predicate<Execution> condition) throws TimeoutException {
        return Await.until(
            () -> executionRepository.findByFlowId(TenantService.MAIN_TENANT, NAMESPACE, flowId, Pageable.from(1, 20)).stream()
                .filter(execution -> execution.getState().isSuccess() && execution.getTrigger() != null)
                .filter(condition)
                .findFirst()
                .orElse(null),
            Duration.ofMillis(500),
            Duration.ofMinutes(1)
        );
    }
}
