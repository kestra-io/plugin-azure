package io.kestra.plugin.azure.datafactory;

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
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Await;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
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


@Slf4j
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
                    subscriptionId: 12345678-1234-1234-1234-12345678abc
                    tenantId: "{{ secret('DATAFACTORY_TENANT_ID') }}"
                    clientId: "{{ secret('DATAFACTORY_CLIENT_ID') }}"
                    clientSecret: "{{ secret('DATAFACTORY_CLIENT_SECRET') }}"
                """
                )
        }
)
@Schema(
        title = "Create a Pipeline run from an Azure Data Factory.",
        description = "Launch an Azure DataFactory pipeline from Kestra. " +
                "Data Factory contains a series of interconnected systems that provide a complete end-to-end platform for data engineers."
)
public class CreateRun extends AbstractDataFactoryConnection implements RunnableTask<CreateRun.Output> {
    private static final String PIPELINE_SUCCEEDED_STATUS = "Succeeded";
    private static final List<String> PIPELINE_FAILED_STATUS = List.of("Failed", "Canceling", "Cancelled");

    @Schema(title = "Factory name")
    private Property<String> factoryName;

    @Schema(title = "Pipeline name")
    private Property<String> pipelineName;

    @Schema(title = "Resource Group name")
    private Property<String> resourceGroupName;

    @Schema(
            title = "Pipeline parameters."
    )
    @Builder.Default
    private Property<Map<String, Object>> parameters = Property.of(new HashMap<>());

    @Schema(
            title = "Wait for the end of the run.",
            description = "Allowing to capture job status & logs."
    )
    @Builder.Default
    private Property<Boolean> wait = Property.of(Boolean.TRUE);

    @Schema(
            title = "The maximum duration to wait for the job completion."
    )
    @Builder.Default
    private Property<Duration> waitUntilCompletion = Property.of(Duration.ofHours(1));

    @Schema(
            title = "Determines how often Kestra should poll the container for completion. By default, the task runner checks every 5 seconds whether the job is completed. You can set this to a lower value (e.g. `PT0.1S` = every 100 milliseconds) for quick jobs and to a lower threshold (e.g. `PT1M` = every minute) for long-running jobs. Setting this property to a lower value will reduce the number of API calls Kestra makes to the remote service — keep that in mind in case you see API rate limit errors."
    )
    @Builder.Default
    private final Property<Duration> completionCheckInterval = Property.of(Duration.ofSeconds(5));

    @Override
    public CreateRun.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        //Authentication
        DataFactoryManager manager = this.getDataFactoryManager(runContext);
        logger.info("Successfully authenticate to Azure Data Factory");

        //Create running pipeline
        final String resourceGroupName = this.resourceGroupName.as(runContext, String.class);
        final String factoryName = this.factoryName.as(runContext, String.class);
        final String pipelineName = this.pipelineName.as(runContext, String.class);

        final var pipelineRunResponse = manager.pipelines()
                .createRunWithResponse(resourceGroupName,
                        factoryName,
                        pipelineName,
                        null,
                        null,
                        null,
                        null,
                        parameters.asMap(runContext, String.class, Object.class),
                        Context.NONE
                );

        if(pipelineRunResponse.getStatusCode() != HttpResponseStatus.OK.code()) {
            throw new RuntimeException("Pipeline run creation failed with status code: " + pipelineRunResponse.getStatusCode());
        }
        logger.info("Created run for pipeline '{}'", pipelineName);

        // Get running pipeline and wait until completion
        final String runId = pipelineRunResponse.getValue().runId();

        if(!Boolean.TRUE.equals(this.wait.as(runContext, Boolean.class))) {
            return Output.builder()
                    .runId(runId)
                    .build();
        }

        final AtomicReference<PipelineRun> runningPipelineResponse = new AtomicReference<>();
        try {
            Await.until(() -> {
                runningPipelineResponse.set(getRunningPipeline(resourceGroupName,factoryName, runId, manager));
                String runStatus = runningPipelineResponse.get().status();

                if (PIPELINE_FAILED_STATUS.contains(runStatus)) {
                    throw new RuntimeException();
                }

                return PIPELINE_SUCCEEDED_STATUS.equals(runStatus);
            }, completionCheckInterval.as(runContext, Duration.class), waitUntilCompletion.as(runContext, Duration.class));
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
            Mono<Long> longMono = FileSerde.writeAll(getIonMapper(), output, flux);
            Long count = longMono.blockOptional().orElse(0L);

            runContext.metric(Counter.of("activities", count));

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
                title = "URI of a kestra internal storage file containing the activities and their inputs/outputs."
        )
        private URI uri;
    }

    private PipelineRun getRunningPipeline(String resourceGroupName, String factoryName, String runId, DataFactoryManager manager) {
        return manager.pipelineRuns().get(resourceGroupName, factoryName, runId);
    }

    private static ObjectMapper getIonMapper() {
        ObjectMapper ionMapper = new ObjectMapper(JacksonMapper.ofIon().getFactory());
        ionMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        ionMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        ionMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        ionMapper.registerModule(new JavaTimeModule());
        return ionMapper;
    }
}
