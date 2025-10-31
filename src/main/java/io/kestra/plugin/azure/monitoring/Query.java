package io.kestra.plugin.azure.monitoring;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.monitor.query.metrics.MetricsClient;
import com.azure.monitor.query.metrics.models.AggregationType;
import com.azure.monitor.query.metrics.models.MetricsQueryResourcesOptions;
import com.azure.monitor.query.metrics.models.MetricsQueryResourcesResult;
import com.azure.monitor.query.metrics.models.MetricsQueryResult;
import com.azure.monitor.query.metrics.models.MetricsQueryTimeInterval;
import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.annotations.*;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
public class Query extends AbstractMonitoringTask implements RunnableTask<Query.Output> {
    @Schema(title = "List of Azure Resource IDs to query metrics from")
    @NotNull
    private Property<List<String>> resourceIds;

    @Schema(title = "List of metric names to query")
    @NotNull
    private Property<List<String>> metricNames;

    @Schema(
        title = "Metrics namespace",
        description = "The namespace of the metrics, e.g., 'Microsoft.Compute/virtualMachines'"
    )
    @NotNull
    private Property<String> metricsNamespace;

    @Schema(
        title = "Time window for metrics",
        description = "Duration looking back from now, e.g., PT5M for 5 minutes"
    )
    @Builder.Default
    private Property<Duration> window = Property.ofValue(Duration.ofMinutes(5));

    @Schema(
        title = "Aggregation types",
        description = "List of aggregation types: Average, Total, Maximum, Minimum, Count"
    )
    private Property<List<String>> aggregations;

    @Schema(
        title = "Time grain for data aggregation",
        description = "ISO 8601 duration format, e.g., PT1M for 1 minute"
    )
    private Property<Duration> interval;

    @Schema(title = "Filter expression to apply to the query")
    private Property<String> filter;

    @Schema(title = "Top N time series to return")
    private Property<Integer> top;

    @Schema(title = "Order by clause for sorting results")
    private Property<String> orderBy;

    @Schema(
        title = "Dimension name(s) to rollup results by",
        description = "For example, 'City' to combine multiple city dimension values into one timeseries"
    )
    private Property<String> rollupBy;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rResourceIds = runContext.render(this.resourceIds).asList(String.class);
        var rMetricNames = runContext.render(this.metricNames).asList(String.class);
        var rMetricsNamespace = runContext.render(this.metricsNamespace).as(String.class).orElseThrow();
        var rWindow = runContext.render(this.window).as(Duration.class).orElse(Duration.ofMinutes(5));
        var rAggregations = runContext.render(this.aggregations).asList(String.class);
        var rInterval = runContext.render(this.interval).as(Duration.class).orElse(null);
        var rFilter = runContext.render(this.filter).as(String.class).orElse(null);
        var rTop = runContext.render(this.top).as(Integer.class).orElse(null);
        var rOrderBy = runContext.render(this.orderBy).as(String.class).orElse(null);
        var rRollupBy = runContext.render(this.rollupBy).as(String.class).orElse(null);
        var endTime = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime startTime = endTime.minus(rWindow);

        AtomicInteger datapoints = new AtomicInteger();
        AtomicInteger metrics = new AtomicInteger();

        MetricsQueryResourcesOptions options = new MetricsQueryResourcesOptions();
        options.setTimeInterval(new MetricsQueryTimeInterval(startTime, endTime));

        if (rAggregations != null && !rAggregations.isEmpty()) {
            List<AggregationType> aggregationTypes = rAggregations.stream()
                .map(AggregationType::fromString)
                .toList();
            options.setAggregations(aggregationTypes);
        }

        if (rInterval != null) {
            options.setGranularity(rInterval);
        }

        if (rFilter != null) {
            options.setFilter(rFilter);
        }

        if (rTop != null) {
            options.setTop(rTop);
        }

        if (rOrderBy != null) {
            options.setOrderBy(rOrderBy);
        }

        if (rRollupBy != null) {
            options.setRollupBy(rRollupBy);
        }

        var client = this.queryClient(runContext);

        Response<MetricsQueryResourcesResult> result = client.queryResourcesWithResponse(rResourceIds, rMetricNames, rMetricsNamespace, options, Context.NONE);

        List<Map<String, Object>> metricsResults = result.getValue()
            .getMetricsQueryResults()
            .stream()
            .peek(r -> {
                for (var m : r.getMetrics()) {
                    metrics.incrementAndGet();
                    m.getTimeSeries().forEach(ts -> datapoints.addAndGet(ts.getValues().size()));
                }
            })
            .map(r -> JacksonMapper.ofJson().convertValue(r, new TypeReference<Map<String, Object>>() {}))
            .toList();


        runContext.logger().info("Fetched {} datapoints across {} metrics from {} resources", datapoints.get(), metrics.get(), result.getValue().getMetricsQueryResults().size());

        return Output.builder()
            .datapoints(datapoints.get())
            .metrics(metrics.get())
            .results(metricsResults)
            .resources(metricsResults.size())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Total number of datapoints fetched across all metrics and resources"
        )
        private final Integer datapoints;

        @Schema(
            title = "Total number of unique metrics fetched across all resources"
        )
        private final Integer metrics;

        @Schema(
            title = "Total number of resources queried"
        )
        private final Integer resources;

        @Schema(
            title = "List of metrics results"
        )
        private final List<Map<String, Object>> results;
    }
}