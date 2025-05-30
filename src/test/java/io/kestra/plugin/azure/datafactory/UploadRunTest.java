package io.kestra.plugin.azure.datafactory;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@KestraTest
class UploadRunTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${kestra.variables.globals.azure.datafactory.tenantId}")
    protected String tenantId;

    @Value("${kestra.variables.globals.azure.datafactory.subscriptionId}")
    protected String subscriptionId;


    @Disabled
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRunPipeline(boolean useCustomDuration) throws Exception {
        final var tenantId = Property.ofValue(this.tenantId);
        final var subscriptionId = Property.ofValue(this.subscriptionId);
        final var factoryName = Property.ofValue("unit-test");
        final var resourceGroupName = Property.ofValue("unit-test");
        final var pipelineName = Property.ofValue("http-test");

        RunContext runContext = runContextFactory.of();

        CreateRun.CreateRunBuilder createRunBuilder = CreateRun.builder()
            .tenantId(tenantId)
            .subscriptionId(subscriptionId)
            .factoryName(factoryName)
            .resourceGroupName(resourceGroupName)
            .pipelineName(pipelineName);

        if (useCustomDuration) {
            createRunBuilder.checkFrequency(CreateRun.CheckFrequency.builder()
                .maxDuration(Property.ofValue(Duration.ofMinutes(1)))
                .interval(Property.ofValue(Duration.ofSeconds(30)))
                .build());
        }

        CreateRun createRun = createRunBuilder.build();

        CreateRun.Output output = createRun.run(runContext);

        //Get logs and outputs
        BufferedReader searchInputStream = new BufferedReader(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())));
        List<Map<String, Object>> results = new ArrayList<>();
        FileSerde.reader(searchInputStream, r -> results.add((Map<String, Object>) r));

        Map<String, Object> logs = results.getLast();
        assertThat(logs.get("status"), is("Succeeded"));
        assertThat(logs.get("activityName"), is("http"));
        assertThat(logs.get("pipelineName"), is(pipelineName.toString()));

        Map<String, Object> outputActivity = (Map<String, Object>) logs.get("output");
        assertThat(outputActivity.get("title"), is("delectus aut autem"));
        assertThat(outputActivity.get("userId"), is(1));
        assertThat(outputActivity.get("id"), is(1));
    }


    @Disabled
    @Test
    void testRunPipelineWithParameter() throws Exception {
        final var tenantId = Property.ofValue(this.tenantId);
        final var subscriptionId = Property.ofValue(this.subscriptionId);
        final var factoryName = Property.ofValue("unit-test");
        final var resourceGroupName = Property.ofValue("unit-test");
        final var pipelineName = Property.ofValue("http-test-with-parameter");
        final var parameters = Property.ofValue(Map.of("pokemonName", (Object) "pikachu"));

        RunContext runContext = runContextFactory.of();

        CreateRun createRun = CreateRun.builder()
                .tenantId(tenantId)
                .subscriptionId(subscriptionId)
                .factoryName(factoryName)
                .resourceGroupName(resourceGroupName)
                .pipelineName(pipelineName)
                .parameters(parameters)
                .build();

        CreateRun.Output output = createRun.run(runContext);

        //Get logs and outputs
        BufferedReader searchInputStream = new BufferedReader(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())));
        List<Map<String, Object>> results = new ArrayList<>();
        FileSerde.reader(searchInputStream, r -> results.add((Map<String, Object>) r));

        Map<String, Object> logs = results.getLast();
        assertThat(logs.get("status"), is("Succeeded"));
        assertThat(logs.get("activityName"), is("http-get-pokemon"));
        assertThat(logs.get("pipelineName"), is(pipelineName.toString()));

        Map<String, Object> outputActivity = (Map<String, Object>) logs.get("output");
        assertThat(outputActivity.get("name"), is("pikachu"));
        assertThat(outputActivity.get("weight"), is(60));
        assertThat(outputActivity.get("id"), is(25));
    }

    @Disabled
    @Test
    void testRunPipelineNoWait() throws Exception {
        final var tenantId = Property.ofValue(this.tenantId);
        final var subscriptionId = Property.ofValue(this.subscriptionId);
        final var factoryName = Property.ofValue("unit-test");
        final var resourceGroupName = Property.ofValue("unittest");
        final var pipelineName = Property.ofValue("http-test");

        RunContext runContext = runContextFactory.of();

        CreateRun createRun = CreateRun.builder()
                .tenantId(tenantId)
                .subscriptionId(subscriptionId)
                .factoryName(factoryName)
                .resourceGroupName(resourceGroupName)
                .wait(Property.ofValue(Boolean.FALSE))
                .pipelineName(pipelineName)
                .build();

        CreateRun.Output output = createRun.run(runContext);

        //Get logs and outputs
        assertThat(output.getUri(), nullValue());
        assertThat(output.getRunId(), notNullValue());
    }
}