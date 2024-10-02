package io.kestra.plugin.azure.function;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class HttpTriggerTest {
    private static final String AZURE_FUNCTION_URI_STRING_OUTPUT = "";
    private static final String AZURE_FUNCTION_URI_LIST_OUTPUT = "";
    private static final String AZURE_FUNCTION_URI_OBJECT_OUTPUT = "";

    @Inject
    private RunContextFactory runContextFactory;

    @Disabled("Disabled with CI/CD, to run the test provide an Azure Function URI")
    @Test
    void testAzureFunctionWithStringOutput() throws Exception {
        HttpTrigger httpTrigger = HttpTrigger.builder()
                .url(Property.of(AZURE_FUNCTION_URI_STRING_OUTPUT + "&firstName=John&name=Doe"))
                .httpMethod(Property.of("GET"))
                .build();

        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        HttpTrigger.Output functionOutput = httpTrigger.run(runContext);

        assertThat((String) functionOutput.getRepsonseBody(), containsString("Hello, JohnDoe"));
    }

    @Disabled("Disabled with CI/CD, to run the test provide an Azure Function URI")
    @Test
    void testAzureFunctionWithJsonListOutput() throws Exception {
        HttpTrigger httpTrigger = HttpTrigger.builder()
            .url(Property.of(AZURE_FUNCTION_URI_LIST_OUTPUT + "&n=6"))
            .httpMethod(Property.of("GET"))
            .build();

        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        HttpTrigger.Output functionOutput = httpTrigger.run(runContext);

        JsonArray fibonacciList = (JsonArray) functionOutput.getRepsonseBody();
        List<Integer> expected = Arrays.asList(0, 1, 1, 2, 3, 5, 8);

        assertThat(fibonacciList.size(), is(expected.size()));

        for (JsonElement jsonElement : fibonacciList) {
            assertThat(expected, hasItem(jsonElement.getAsInt()));
        }
    }

    @Disabled("Disabled with CI/CD, to run the test provide an Azure Function URI")
    @Test
    void testAzureFunctionWithBodyAndJsonObjectOutput() throws Exception {
        HttpTrigger httpTrigger = HttpTrigger.builder()
            .url(Property.of(AZURE_FUNCTION_URI_OBJECT_OUTPUT ))
            .httpBody(Property.of(Map.of("text", "Hello, Kestra")))
            .httpMethod(Property.of("POST"))
            .build();

        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        HttpTrigger.Output functionOutput = httpTrigger.run(runContext);

        JsonObject objectResult = (JsonObject) functionOutput.getRepsonseBody();
        assertThat(objectResult.get("originalText").getAsString(), is("Hello, Kestra"));
        assertThat(objectResult.get("encodedText").getAsString(), is("SGVsbG8sIEtlc3RyYQ=="));
    }
}