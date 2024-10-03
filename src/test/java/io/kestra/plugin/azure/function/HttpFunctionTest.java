package io.kestra.plugin.azure.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
class HttpFunctionTest {
    private static final String AZURE_FUNCTION_URI_STRING_OUTPUT = "";

    private static final String AZURE_FUNCTION_URI_LIST_OUTPUT = "";

    private static final String AZURE_FUNCTION_URI_OBJECT_OUTPUT = "";

    @Inject
    private RunContextFactory runContextFactory;

    @Disabled("Disabled with CI/CD, to run the test provide an Azure Function URI")
    @Test
    void testAzureFunctionWithStringOutput() throws Exception {
        HttpFunction httpFunction = HttpFunction.builder()
                .url(Property.of(AZURE_FUNCTION_URI_STRING_OUTPUT + "&firstName=John&name=Doe"))
                .httpMethod(Property.of("GET"))
                .build();

        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        HttpFunction.Output functionOutput = httpFunction.run(runContext);

        assertThat((String) functionOutput.getRepsonseBody(), containsString("Hello, JohnDoe"));
    }

    @Disabled("Disabled with CI/CD, to run the test provide an Azure Function URI")
    @Test
    void testAzureFunctionWithJsonListOutput() throws Exception {
        HttpFunction httpFunction = HttpFunction.builder()
            .url(Property.of(AZURE_FUNCTION_URI_LIST_OUTPUT + "&n=6"))
            .httpMethod(Property.of("GET"))
            .build();

        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        HttpFunction.Output functionOutput = httpFunction.run(runContext);

        ArrayNode fibonacciList = (ArrayNode) functionOutput.getRepsonseBody();
        List<Integer> expected = Arrays.asList(0, 1, 1, 2, 3, 5, 8);

        assertThat(fibonacciList.size(), is(expected.size()));

        for (JsonNode jsonElement : fibonacciList) {
            assertThat(expected, hasItem(jsonElement.asInt()));
        }
    }

    @Disabled("Disabled with CI/CD, to run the test provide an Azure Function URI")
    @Test
    void testAzureFunctionWithBodyAndJsonObjectOutput() throws Exception {
        HttpFunction httpFunction = HttpFunction.builder()
            .url(Property.of(AZURE_FUNCTION_URI_OBJECT_OUTPUT ))
            .httpBody(Property.of(Map.of("text", "Hello, Kestra")))
            .httpMethod(Property.of("POST"))
            .build();

        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        HttpFunction.Output functionOutput = httpFunction.run(runContext);

        JsonNode objectResult = (JsonNode) functionOutput.getRepsonseBody();
        assertThat(objectResult.get("originalText").asText(), is("Hello, Kestra"));
        assertThat(objectResult.get("encodedText").asText(), is("SGVsbG8sIEtlc3RyYQ=="));
    }
}