package io.kestra.plugin.azure.monitoring;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

@KestraTest
@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
@Disabled("To run this test provide your service principal credentials")
public class TriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.variables.globals.azure.monitoring.tenantId}")
    protected String tenantId;

    @Value("${kestra.variables.globals.azure.monitoring.resourceId}")
    protected String resourceId;

    @Test
    void shouldTriggerWhenMetricsExist() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("monitor-" + System.currentTimeMillis())
            .type(Trigger.class.getName())
            .tenantId(Property.ofValue(tenantId))
            .endpoint(Property.ofValue("some/endpoint"))
            .resourceIds(Property.ofValue(List.of(resourceId)))
            .metricNames(Property.ofValue(List.of("Percentage CPU")))
            .metricsNamespace(Property.ofValue("Microsoft.Compute/virtualMachines"))
            .window(Property.ofValue(Duration.ofMinutes(5)))
            .aggregations(Property.ofValue(List.of("Average")))
            .interval(Duration.ofMinutes(1))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        Execution exec = execution.get();

        assertThat(exec.getTrigger().getId(), startsWith("monitor-"));
        assertThat(exec.getTrigger().getVariables(), hasKey("datapoints"));
        assertThat((Integer) exec.getTrigger().getVariables().get("datapoints"), greaterThan(0));

        List<Map<String, Object>> results = (List<Map<String, Object>>) exec.getTrigger().getVariables().get("results");
        assertThat(results, not(empty()));
        assertThat(results.getFirst(), hasKey("metrics"));
    }
}
