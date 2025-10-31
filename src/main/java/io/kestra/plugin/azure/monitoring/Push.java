package io.kestra.plugin.azure.monitoring;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Query metrics from Azure Monitor.")
@Plugin(
    examples = {
        @Example(
            title = "Query CPU utilization from Azure Monitor for multiple VMs",
            code = """
                id: azure_monitor_query
                namespace: company.team
                tasks:
                  - id: query
                    type: io.kestra.plugin.azure.monitoring.Query
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    resourceIds:
                      - "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm1"
                      - "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm2"
                    metricNames:
                      - "Percentage CPU"
                    metricsNamespace: "Microsoft.Compute/virtualMachines"
                    window: PT5M
                    aggregations:
                      - "Average"
                      - "Maximum"
                """
        )
    }
)
public class Push extends AbstractMonitoringTask implements RunnableTask<Push.Output> {
    @Schema(title = "DCR ingestion path")
    @NotNull
    private Property<String> path;

    @Schema(title = "Metric data body")
    @NotNull
    private Property<Map<String, Object>> metrics;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rPath = runContext.render(path).as(String.class).orElseThrow();
        var rMetrics = runContext.render(metrics).asMap(String.class, Object.class);

        var response = postToIngestion(runContext, rPath, rMetrics);

        runContext.logger().info("Ingestion request completed with status {}", response.getStatus());

        return Output.builder()
            .body(response.getBody())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private final Map<String, Object> body;
    }
}
