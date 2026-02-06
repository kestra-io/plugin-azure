package io.kestra.plugin.azure.datafactory;

import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.core.util.Context;
import com.azure.resourcemanager.datafactory.DataFactoryManager;
import com.azure.resourcemanager.datafactory.models.ActivityRun;
import com.azure.resourcemanager.datafactory.models.PipelineRun;
import com.azure.resourcemanager.datafactory.models.RunFilterParameters;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Await;
import io.kestra.plugin.azure.AbstractAzureIdentityConnection;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: azure_datafactory_create_run
                namespace: company.team

                tasks:
                  - id: create_run
                    type: io.kestra.plugin.azure.datafactory.CreateRun
                    factoryName: exampleFactoryName
                    pipelineName: examplePipeline
                    resourceGroupName: exampleResourceGroup
                    subscriptionId: 12345678-1234-1234-12345678abc
                    tenantId: "{{ secret('DATAFACTORY_TENANT_ID') }}"
                    clientId: "{{ secret('DATAFACTORY_CLIENT_ID') }}"
                    clientSecret: "{{ secret('DATAFACTORY_CLIENT_SECRET') }}"
                """
        )
    },
    metrics = {
        @Metric(name = "pipeline.duration.ms", type = Timer.TYPE, description = "The duration of the pipeline run in milliseconds."),
        @Metric(name = "activities.count", type = Counter.TYPE, description = "The total number of activities in the pipeline run.")
    }
)
@Schema(
    title = "Start an Azure Data Factory pipeline run",
    description = "Triggers a Data Factory pipeline and optionally waits for completion, emitting run metrics and writing activity logs to internal storage. Defaults: wait=true, polling interval=PT5S, timeout=PT1H; task fails on Failed/Cancelled pipeline states."
)
public class CreateRun extends AbstractAzureIdentityConnection implements RunnableTask<CreateRun.Output> {
    private static final String PIPELINE_SUCCEEDED_STATUS = "Succeeded";
    private static final List<String> PIPELINE_FAILED_STATUS = List.of("Failed", "Canceling", "Cancelled");

    @Schema(title = "Subscription ID", description = "Azure subscription GUID that owns the Data Factory")
    @NotNull
    protected Property<String> subscriptionId;

    @Schema(title = "Factory name", description = "Azure Data Factory name")
    private Property<String> factoryName;

    @Schema(title = "Pipeline name", description = "Pipeline to trigger inside the factory")
    private Property<String> pipelineName;

    @Schema(title = "Resource group name", description = "Resource group containing the Data Factory")
    private Property<String> resourceGroupName;

    @Schema(title = "Pipeline parameters", description = "Key/value parameters passed to the pipeline run")
    @Builder.Default
    private Property<Map<String, Object>> parameters = Property.ofValue(new HashMap<>());

    @Schema(title = "Wait for completion", description = "If true (default), poll pipeline status and collect activity logs; false returns runId immediately")
    @Builder.Default
    private Property<Boolean> wait = Property.ofValue(Boolean.TRUE);

    @Schema(title = "Polling frequency", description = "Interval and max duration used when wait=true")
    @PluginProperty
    @Builder.Default
    private CheckFrequency checkFrequency = CheckFrequency.builder().build();

    @Override
    public CreateRun.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        //Authentication
        DataFactoryManager manager = this.dataFactoryManager(runContext);
        logger.info("Successfully authenticate to Azure Data Factory");

        //Create running pipeline
        final String resourceGroupName = runContext.render(this.resourceGroupName).as(String.class).orElse(null);
        final String factoryName = runContext.render(this.factoryName).as(String.class).orElse(null);
        final String pipelineName = runContext.render(this.pipelineName).as(String.class).orElse(null);

        final var pipelineRunResponse = manager.pipelines()
                .createRunWithResponse(resourceGroupName,
                        factoryName,
                        pipelineName,
                        null,
                        null,
                        null,
                        null,
                        runContext.render(this.parameters).asMap(String.class, Object.class),
                        Context.NONE
                );

        if(pipelineRunResponse.getStatusCode() != HttpResponseStatus.OK.code()) {
            throw new RuntimeException("Pipeline run creation failed with status code: " + pipelineRunResponse.getStatusCode());
        }
        logger.info("Created run for pipeline '{}'", pipelineName);

        // Get running pipeline and wait until completion
        final String runId = pipelineRunResponse.getValue().runId();

        if(!Boolean.TRUE.equals(runContext.render(this.wait).as(Boolean.class).orElseThrow())) {
            return Output.builder()
                    .runId(runId)
                    .build();
        }

        final Duration checkInterval = runContext.render(this.checkFrequency.getInterval()).as(Duration.class).orElseThrow();
        final Duration maxDuration = runContext.render(this.checkFrequency.getMaxDuration()).as(Duration.class).orElseThrow();
        final AtomicReference<PipelineRun> runningPipelineResponse = new AtomicReference<>();
        try {
            Await.until(() -> {
                runningPipelineResponse.set(runningPipeline(resourceGroupName,factoryName, runId, manager));
                String runStatus = runningPipelineResponse.get().status();

                if (PIPELINE_FAILED_STATUS.contains(runStatus)) {
                    throw new RuntimeException();
                }

                return PIPELINE_SUCCEEDED_STATUS.equals(runStatus);
            }, checkInterval, maxDuration);
        } catch (TimeoutException | RuntimeException e) {
            logger.error("Pipeline '{}' with runId '{} finished with status '{}'", pipelineName, runId, runningPipelineResponse.get().status());
            throw new RuntimeException(runningPipelineResponse.get().message());
        }

        final PipelineRun pipelineRun = runningPipelineResponse.get();
        logger.info("Pipeline '{}' with runId '{} finished with status '{}'", pipelineName, runId, pipelineRun.status());
        runContext.metric(Timer.of("pipeline.duration.ms", Duration.of(pipelineRun.durationInMs(), ChronoUnit.MILLIS)));

        //Get all the logs for each activity in the pipeline
        final var activitiesResponse = manager.activityRuns().queryByPipelineRunWithResponse(
                resourceGroupName,
                factoryName,
                runId,
                new RunFilterParameters()
                        .withLastUpdatedAfter(pipelineRun.runStart())
                        .withLastUpdatedBefore(pipelineRun.runEnd()),
                com.azure.core.util.Context.NONE);

        if(activitiesResponse.getStatusCode() != HttpResponseStatus.OK.code()) {
            throw new RuntimeException("Query pipeline run activities failed with status code: " + activitiesResponse.getStatusCode());
        }

        List<ActivityRun> activities = activitiesResponse.getValue().value();
        logger.info("Logging pipeline activities");
        activities.forEach(activityRun -> {
            if(activityRun.status().equals(PIPELINE_SUCCEEDED_STATUS)) {
                logger.info("Activity '{}' finished with status '{}'", activityRun.activityName(), activityRun.status());
            } else {
                logger.error("Activity '{}' finished with status '{}'", activityRun.activityName(), activityRun.status());
            }
        });

        //Store activities logs and outputs
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var output = new BufferedWriter(new FileWriter(tempFile))) {
            var flux = Flux.fromIterable(activities);
            Mono<Long> longMono = FileSerde.writeAll(ionMapper(), output, flux);
            Long count = longMono.blockOptional().orElse(0L);

            runContext.metric(Counter.of("activities.count", count));

            return Output.builder()
                    .runId(runId)
                    .uri(runContext.storage().putFile(tempFile))
                    .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Run ID", description = "The ID of the pipeline run created in Azure Data Factory")
        private String runId;

        @Schema(
            title = "Activities log URI",
            description = "kestra:// URI for an Ion file containing activity runs with inputs/outputs when wait=true"
        )
        private URI uri;
    }

    @Builder
    @Getter
    public static class CheckFrequency {
        @Schema(
            title = "Max wait duration",
            description = "Stop polling and fail after this duration; defaults to PT1H"
        )
        @Builder.Default
        private Property<Duration> maxDuration = Property.ofValue(Duration.ofHours(1));

        @Schema(
            title = "Polling interval",
            description = "Delay between status checks; defaults to PT5S"
        )
        @Builder.Default
        private Property<Duration> interval = Property.ofValue(Duration.ofSeconds(5));
    }

    private DataFactoryManager dataFactoryManager(RunContext runContext) throws IllegalVariableEvaluationException {
        runContext.logger().info("Authenticating to Azure Data Factory");
        return DataFactoryManager.authenticate(credentials(runContext), profile(runContext));
    }

    public AzureProfile profile(RunContext runContext) throws IllegalVariableEvaluationException {
        final String tenantId = runContext.render(this.tenantId).as(String.class).orElse(null);
        final String subscriptionId = runContext.render(this.subscriptionId).as(String.class).orElse(null);

        return  new AzureProfile(
            tenantId,
            subscriptionId,
            AzureEnvironment.AZURE
        );
    }

    private PipelineRun runningPipeline(String resourceGroupName, String factoryName, String runId, DataFactoryManager manager) {
        return manager.pipelineRuns().get(resourceGroupName, factoryName, runId);
    }

    private static ObjectMapper ionMapper() {
        ObjectMapper ionMapper = new ObjectMapper(JacksonMapper.ofIon().getFactory());
        ionMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        ionMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        ionMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        ionMapper.registerModule(new JavaTimeModule());
        return ionMapper;
    }
}
