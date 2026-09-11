package io.kestra.plugin.azure.monitoring;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Push a record to Azure Monitor",
    description = "Sends a record to a Log Analytics table through the Logs Ingestion API, using a Data Collection Rule and Azure AD authentication. The record must match the schema the rule declares for the target stream."
)
@Plugin(
    examples = {
        @Example(
            title = "Push a record to Azure Monitor via a Data Collection Rule",
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
                    endpoint: "https://my-dce-a1b2.westeurope.ingest.monitor.azure.com"
                    path: "/dataCollectionRules/dcr-xxxxxxxxxxxxxxxx/streams/Custom-MyStream"
                    metrics:
                      TimeGenerated: "2024-01-01T00:00:00Z"
                      Computer: "worker-01"
                      AdditionalContext: "orders processed"
                """
        )
    }
)
public class Push extends AbstractMonitoringTask implements RunnableTask<Push.Output> {
    // tolerates a full URL and a trailing ?api-version=, which the hand-rolled version required callers to append
    private static final Pattern DCR_PATH = Pattern.compile("/dataCollectionRules/(?<ruleId>[^/?]+)/streams/(?<stream>[^/?]+)");

    @Schema(title = "DCR ingestion path", description = "Path portion of the Data Collection Rule ingestion URL (e.g., /dataCollectionRules/{id}/streams/{stream})")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> path;

    @Schema(title = "Record to ingest", description = "JSON object matching the schema the Data Collection Rule declares for the target stream")
    @NotNull
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> metrics;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rPath = runContext.render(path).as(String.class).orElseThrow();
        var rMetrics = runContext.render(metrics).asMap(String.class, Object.class);

        if (rMetrics == null || rMetrics.isEmpty()) {
            throw new IllegalArgumentException("metrics is required and must contain at least one field");
        }

        var matcher = DCR_PATH.matcher(rPath);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                "path must be a Data Collection Rule ingestion path of the form /dataCollectionRules/{immutableId}/streams/{stream}, got '%s'".formatted(rPath)
            );
        }

        var ruleId = matcher.group("ruleId");
        var stream = matcher.group("stream");

        try (var client = ingestionClient(runContext)) {
            client.upload(ruleId, stream, List.of(rMetrics));
        }

        runContext.logger().info("Ingested 1 record into stream {} of rule {}", stream, ruleId);

        return Output.builder().build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        // the Logs Ingestion API answers 204 with no content, kept so existing flows referencing it still resolve
        @Schema(title = "Ingestion response body", description = "Always null, the Logs Ingestion API returns no content on success")
        private final Map<String, Object> body;
    }
}
