package io.kestra.plugin.azure.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger an Azure Function.",
    description = "Use this task to trigger an Azure Function and collect the result."
)
@Plugin(examples = {
    @Example(
        full = true,
        code = """
            id: test_azure_function
            namespace: com.company.test.azure

            tasks:
              - id: encode_string
                type: io.kestra.plugin.azure.function.HttpFunction
                httpMethod: POST
                url: https://service.azurewebsites.net/api/Base64Encoder?code=${{secret('AZURE_FUNCTION_CODE')}}
                httpBody: {"text": "Hello, Kestra"}
            """
    )
})
public class HttpFunction extends Task implements RunnableTask<HttpFunction.Output> {
    @Schema(title = "HTTP method")
    @NotNull
    protected Property<String> httpMethod;

    @Schema(title = "Azure Function URL")
    @NotNull
    protected Property<String> url;

    @Schema(
            title = "HTTP body",
            description = "JSON body of the Azure Function"
    )
    @Builder.Default
    protected Property<Map<String, Object>> httpBody = Property.ofValue(new HashMap<>());

    @Schema(
            title = "Max duration",
            description = "The maximum duration the task should wait until the Azure Function completes."
    )
    @Builder.Default
    @PluginProperty(dynamic = true)
    protected Property<Duration> maxDuration = Property.ofValue(Duration.ofMinutes(60));

    @Override
    public HttpFunction.Output run(RunContext runContext) throws Exception {
        String rUrl = runContext.render(url).as(String.class).orElseThrow();
        String rMethod = runContext.render(httpMethod).as(String.class).orElseThrow();
        Map<String, Object> rBody = runContext.render(httpBody).asMap(String.class, Object.class);
        Duration timeout = runContext.render(maxDuration).as(Duration.class).orElseThrow();

        try (HttpClient client = HttpClient.builder()
                .runContext(runContext)
                .configuration(HttpConfiguration.builder()
                    .readTimeout(timeout)
                    .build())
                .build()) {

            HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
                .uri(URI.create(rUrl))
                .method(rMethod);

            if (rBody != null && !rBody.isEmpty()) {
                requestBuilder.body(HttpRequest.JsonRequestBody.builder()
                    .content(rBody)
                    .build());
            }

            HttpResponse<String> response = client.request(requestBuilder.build());
            String body = response.getBody() != null ? response.getBody() : "";

            try {
                ObjectMapper mapper = new ObjectMapper();
                return Output.builder()
                    .repsonseBody(mapper.readTree(body))
                    .build();
            } catch (Exception e) {
                return Output.builder()
                    .repsonseBody(body)
                    .build();
            }
        } catch (HttpClientException | IOException e) {
            throw new RuntimeException(
                    "Request failed with error: " + e.getMessage(),
                    e
            );
        } catch (IllegalVariableEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    @Builder
    @Getter
    static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Response body")
        private Object repsonseBody;
    }
}
