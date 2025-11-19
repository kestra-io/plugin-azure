package io.kestra.plugin.azure.monitoring;

import io.kestra.core.models.annotations.*;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Trigger a flow when Azure Monitor metrics match a query condition.")
@Plugin(
    examples = {
        @Example(
            title = "Trigger when Azure Monitor metric query returns non-empty results",
            full = true,
            code = """
                id: azure_monitor_trigger
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.series }}"
                    tasks:
                      - id: log
                        type: io.kestra.plugin.core.log.Log
                        message: "Datapoint: {{ json(taskrun.value) }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.azure.monitoring.Trigger
                    interval: "PT1M"
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    resourceIds:
                      - "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm1"
                    metricNames:
                      - "Percentage CPU"
                    metricsNamespace: "Microsoft.Compute/virtualMachines"
                    window: PT5M
                    aggregations:
                      - "Average"
                """
        ),
        @Example(
            title = "Trigger when CPU exceeds threshold with filter",
            full = true,
            code = """
                id: azure_monitor_cpu_alert
                namespace: company.team

                tasks:
                  - id: alert
                    type: io.kestra.plugin.core.log.Log
                    message: "High CPU detected: {{ trigger.count }} datapoints"

                triggers:
                  - id: watch_cpu
                    type: io.kestra.plugin.azure.monitoring.Trigger
                    interval: "PT5M"
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
                    filter: "Average gt 80"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Query.Output> {
    protected Property<String> tenantId;

    protected Property<String> clientId;

    protected Property<String> clientSecret;

    protected Property<String> subscriptionId;

    @NotNull
    protected Property<String> endpoint;

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private Property<List<String>> resourceIds;

    private Property<List<String>> metricNames;

    private Property<String> metricsNamespace;

    @Builder.Default
    private Property<Duration> window = Property.ofValue(Duration.ofMinutes(5));

    private Property<List<String>> aggregations;

    private Property<Duration> granularity;

    private Property<String> filter;

    private Property<Integer> top;

    private Property<String> orderBy;

    private Property<String> rollupBy;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Query.Output output = Query.builder()
            .id(this.id)
            .type(Query.class.getName())
            .tenantId(this.tenantId)
            .clientId(this.clientId)
            .clientSecret(this.clientSecret)
            .resourceIds(this.resourceIds)
            .metricNames(this.metricNames)
            .metricsNamespace(this.metricsNamespace)
            .window(this.window)
            .aggregations(this.aggregations)
            .interval(this.granularity)
            .filter(this.filter)
            .top(this.top)
            .orderBy(this.orderBy)
            .rollupBy(this.rollupBy)
            .endpoint(this.endpoint)
            .build()
            .run(runContext);

        runContext.logger().info("Fetched {} datapoints across {} metrics from {} resources", output.getDatapoints(), output.getMetrics(), output.getResults().size());

        if (output.getDatapoints() == 0) {
            return Optional.empty();
        }

        return Optional.of(
            TriggerService.generateExecution(this, conditionContext, context, output)
        );
    }
}