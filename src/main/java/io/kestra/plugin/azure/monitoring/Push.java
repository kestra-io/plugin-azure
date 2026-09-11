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
    // anchored so a path with extra segments is not silently truncated, the trailing query is what callers had to append themselves
    private static final Pattern DCR_PATH = Pattern.compile("/dataCollectionRules/(?<ruleId>[^/?]+)/streams/(?<stream>[^/?]+)/?(\\?.*)?");

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

        if (rMetrics == null) {
            throw new IllegalArgumentException("metrics is required");
        }

        var matcher = DCR_PATH.matcher(rPath);
        if (!matcher.matches()) {
            // any other Azure Monitor ingestion endpoint still goes out as it did before, Azure decides if it is valid
            var response = postVerbatim(runContext, rPath, rMetrics);
            runContext.logger().info("Ingestion request completed with status {}", response.getStatus());

            return Output.builder().body(response.getBody()).build();
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
        @Schema(title = "Ingestion response body", description = "Null for Data Collection Rule paths, the Logs Ingestion API returns no content. Otherwise the raw response body")
        private final Map<String, Object> body;
    }
}
