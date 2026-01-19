package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCosmosContainerTask<T extends Output> extends AbstractCosmosDatabaseTask<T> {
    @NotNull
    @Schema(title = "container ID")
    private Property<String> containerId;

    @Override
    protected T run(RunContext runContext, CosmosAsyncDatabase cosmosDatabase) throws Exception {
        String rContainerId = runContext.render(containerId).as(String.class).orElseThrow(
            () -> new IllegalVariableEvaluationException("container id needed")
        );

        CosmosAsyncContainer cosmosContainer = cosmosDatabase.getContainer(rContainerId);
        return run(runContext, cosmosContainer);
    }

    abstract protected T run(RunContext runContext, CosmosAsyncContainer cosmosContainer) throws Exception;

    public record ItemResponseOutput<T>(
        T item,
        String maxResourceQuota,
        String currentResourceQuotaUsage,
        String activityId,
        double requestCharge,
        int statusCode,
        String sessionToken,
        Map<String, String> responseHeaders,
        CosmosDiagnostics diagnostics,
        Duration duration,
        String eTag
    ) implements io.kestra.core.models.tasks.Output {
        public static <T> ItemResponseOutput<T> from(com.azure.cosmos.models.CosmosItemResponse<T> r) {
            return new ItemResponseOutput<>(
                r.getItem(),
                r.getMaxResourceQuota(),
                r.getCurrentResourceQuotaUsage(),
                r.getActivityId(),
                r.getRequestCharge(),
                r.getStatusCode(),
                r.getSessionToken(),
                r.getResponseHeaders(),
                r.getDiagnostics(),
                r.getDuration(),
                r.getETag()
            );
        }
    }
}
