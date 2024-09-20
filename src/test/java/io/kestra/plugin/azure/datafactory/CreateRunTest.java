package io.kestra.plugin.azure.datafactory;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@KestraTest
class CreateRunTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${kestra.variables.globals.azure.datafactory.tenantId}")
    protected String tenantId;

    @Value("${kestra.variables.globals.azure.datafactory.subscriptionId}")
    protected String subscriptionId;


    @Disabled
    @Test
    void testRunPipeline() throws Exception {
        final var tenantId = Property.of(this.tenantId);
        final var subscriptionId = Property.of(this.subscriptionId);
        final var factoryName = Property.of("unit-test");
        final var resourceGroupName = Property.of("unittest");
        final var pipelineName = Property.of("http-test");

        RunContext runContext = runContextFactory.of();

        CreateRun createRun = CreateRun.builder()
                .tenantId(tenantId)
                .subscriptionId(subscriptionId)
                .factoryName(factoryName)
                .resourceGroupName(resourceGroupName)
                .pipelineName(pipelineName)
                .build();

        CreateRun.Output output = createRun.run(runContext);

        //Get logs and outputs
        BufferedReader searchInputStream = new BufferedReader(new InputStreamReader(storageInterface.get(null, output.getUri())));
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
        final var tenantId = Property.of(this.tenantId);
        final var subscriptionId = Property.of(this.subscriptionId);
        final var factoryName = Property.of("unit-test");
        final var resourceGroupName = Property.of("unittest");
        final var pipelineName = Property.of("http-test-with-parameter");
        final var parameters = Property.of(Map.of("pokemonName", (Object) "pikachu"));

        RunContext runContext = runContextFactory.of();

        CreateRun createRun = CreateRun.builder()
                .tenantId(tenantId)
                .subscriptionId(subscriptionId)
                .factoryName(factoryName)
                .resourceGroupName(resourceGroupName)
                .pipelineName(pipelineName)
                .parameters(parameters)
                .completionCheckInterval(Property.of(Duration.ofSeconds(5L)))
                .waitUntilCompletion(Property.of(Duration.ofSeconds(30L)))
                .build();

        CreateRun.Output output = createRun.run(runContext);

        //Get logs and outputs
        BufferedReader searchInputStream = new BufferedReader(new InputStreamReader(storageInterface.get(null, output.getUri())));
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
}