package io.kestra.plugin.azure.monitoring;

import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Push metrics to Azure Monitor",
    description = "Posts custom metrics payloads to the Metrics Ingestion endpoint using Azure AD authentication. Requires regional endpoint and DCR ingestion path."
)
@Plugin(
    examples = {
        @Example(
            title = "Push a custom metric to Azure Monitor via a Data Collection Rule",
            full = true,
            code = """
                id: azure_monitor_push
                namespace: company.team
                tasks:
                  - id: push
                    type: io.kestra.plugin.azure.monitoring.Push
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    endpoint: "https://westeurope.metrics.monitor.azure.com"
                    path: "/dataCollectionRules/dcr-xxxxxxxxxxxxxxxx/streams/Custom-MyStream"
                    metrics:
                      time: "2024-01-01T00:00:00Z"
                      data:
                        baseData:
                          metric: "OrdersProcessed"
                          namespace: "MyCompany.Orders"
                          dimNames:
                            - "Environment"
                          series:
                            - dimValues:
                                - "Production"
                              sum: 100
                              count: 5
                """
        )
    }
)
public class Push extends AbstractMonitoringTask implements RunnableTask<Push.Output> {
    @Schema(title = "DCR ingestion path", description = "Path portion of the Data Collection Rule ingestion URL (e.g., /dataCollectionRules/{id}/streams/{stream})")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> path;

    @Schema(title = "Metric data body", description = "JSON payload formatted for Azure Monitor ingestion API")
    @NotNull
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> metrics;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rPath = runContext.render(path).as(String.class).orElseThrow();
        var rMetrics = runContext.render(metrics).asMap(String.class, Object.class);

        var response = ingestMetrics(runContext, rPath, rMetrics);

        runContext.logger().info("Ingestion request completed with status {}", response.getStatus());

        return Output.builder()
            .body(response.getBody())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Ingestion response body", description = "Raw JSON returned by the ingestion API")
        private final Map<String, Object> body;
    }
}
