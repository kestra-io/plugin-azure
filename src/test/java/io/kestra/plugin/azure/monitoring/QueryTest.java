package io.kestra.plugin.azure.monitoring;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
@Disabled("To run this test provide your service principal credentials")
class QueryTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.variables.globals.azure.monitoring.tenantId}")
    protected String tenantId;

    @Value("${kestra.variables.globals.azure.monitoring.clientId}")
    protected String clientId;

    @Value("${kestra.variables.globals.azure.monitoring.clientSecret}")
    protected String clientSecret;

    @Value("${kestra.variables.globals.azure.monitoring.resourceId}")
    protected String resourceId;

    @Test
    void testQueryResource() throws Exception {
        RunContext runContext = runContextFactory.of();

        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .tenantId(Property.ofValue(this.tenantId))
            .clientId(Property.ofValue(this.clientId))
            .clientSecret(Property.ofValue(this.clientSecret))
            .resourceIds(Property.ofValue(List.of(this.resourceId)))
            .metricNames(Property.ofValue(List.of("Percentage CPU")))
            .metricsNamespace(Property.ofValue("Microsoft.Compute/virtualMachines"))
            .window(Property.ofValue(Duration.ofMinutes(5)))
            .aggregations(Property.ofValue(List.of("Average", "Maximum")))
            .build();

        Query.Output output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getDatapoints(), greaterThanOrEqualTo(0));
        assertThat(output.getResources(), notNullValue());

        Map<String, Object> firstResult = output.getResults().getFirst();
        assertThat(firstResult, hasKey("metrics"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> metrics = (List<Map<String, Object>>) firstResult.get("metrics");
        assertThat(metrics, not(empty()));

        Map<String, Object> firstMetric = metrics.getFirst();
        assertThat(firstMetric, hasKey("metricName"));
        assertThat(firstMetric.get("metricName"), is("Percentage CPU"));
    }
}