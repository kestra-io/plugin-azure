package io.kestra.plugin.azure.synapse;

import com.azure.analytics.synapse.spark.SparkBatchClient;
import com.azure.analytics.synapse.spark.SparkClientBuilder;
import com.azure.analytics.synapse.spark.models.SparkBatchJob;
import com.azure.analytics.synapse.spark.models.SparkBatchJobOptions;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.AbstractAzureIdentityConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

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
                id: azure_synapse_spark_batch
                namespace: company.team

                tasks:
                  - id: spark_job
                    type: io.kestra.plugin.azure.synapse.SparkBatchJobCreate
                    endpoint: "https://myworkspace.dev.azuresynapse.net"
                    sparkPoolName: "mysparkpool"
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    name: "WordCount"
                    file: "abfss://data@mystorage.dfs.core.windows.net/jars/wordcount.jar"
                    className: "org.example.WordCount"
                    arguments:
                      - "abfss://data@mystorage.dfs.core.windows.net/input/shakespeare.txt"
                      - "abfss://data@mystorage.dfs.core.windows.net/output/"
                    driverMemory: "28g"
                    driverCores: 4
                    executorMemory: "28g"
                    executorCores: 4
                    executorCount: 2
                """
        )
    }
)
@Schema(
    title = "Create and submit a Spark batch job to Azure Synapse Analytics Spark pool.",
    description = "This task submits a Spark batch job to an Azure Synapse Analytics Spark pool."
)
public class SparkBatchJobCreate extends AbstractAzureIdentityConnection implements RunnableTask<SparkBatchJobCreate.Output> {

    @Schema(
        title = "The Synapse workspace endpoint.",
        description = "Endpoint should be in the format: https://{YOUR_WORKSPACE_NAME}.dev.azuresynapse.net"
    )
    @NotNull
    private Property<String> rEndpoint;

    @Schema(
        title = "The name of the Spark pool.",
        description = "The Spark pool where the batch job will be submitted."
    )
    @NotNull
    private Property<String> rSparkPoolName;

    @Schema(
        title = "The name of the Spark batch job."
    )
    @NotNull
    private Property<String> rName;

    @Schema(
        title = "The main file used for the job.",
        description = "Path to the main application file (JAR, Python, etc.) in ADLS Gen2 storage. " +
            "Format: abfss://{container}@{storage-account}.dfs.core.windows.net/path/to/file"
    )
    @NotNull
    private Property<String> rFile;

    @Schema(
        title = "The fully qualified class name for Java/Scala Spark jobs.",
        description = "Main class to be executed. Required for Java/Scala jobs."
    )
    private Property<String> rClassName;

    @Schema(
        title = "Command line arguments for the Spark job.",
        description = "List of arguments passed to the main method."
    )
    private Property<List<String>> rArguments;

    @Schema(
        title = "Additional JAR files.",
        description = "List of additional JARs to be used in the job."
    )
    private Property<List<String>> rJars;

    @Schema(
        title = "Additional Python files.",
        description = "List of additional Python files to be used in the job."
    )
    private Property<List<String>> rPyFiles;

    @Schema(
        title = "Additional files.",
        description = "List of additional files to be used in the job."
    )
    private Property<List<String>> rFiles;

    @Schema(
        title = "Archives to be used in the job.",
        description = "List of archives to be used in the job."
    )
    private Property<List<String>> rArchives;

    @Schema(
        title = "Spark configuration properties.",
        description = "Map of Spark configuration properties to be set for the job."
    )
    private Property<Map<String, String>> rConf;

    @Schema(
        title = "Driver memory size.",
        description = "Amount of memory to use for the driver process (e.g., '28g')."
    )
    private Property<String> rDriverMemory;

    @Schema(
        title = "Number of CPU cores for the driver.",
        description = "Number of cores to use for the driver process."
    )
    private Property<Integer> rDriverCores;

    @Schema(
        title = "Executor memory size.",
        description = "Amount of memory to use per executor process (e.g., '28g')."
    )
    private Property<String> rExecutorMemory;

    @Schema(
        title = "Number of CPU cores per executor.",
        description = "Number of cores to use for each executor."
    )
    private Property<Integer> rExecutorCores;

    @Schema(
        title = "Number of executors.",
        description = "Number of executor processes to launch for this job."
    )
    private Property<Integer> rExecutorCount;

    @Schema(
        title = "Tags for the job.",
        description = "Map of tags to associate with the job."
    )
    private Property<Map<String, String>> rTags;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String endpoint = runContext.render(this.rEndpoint).as(String.class).orElseThrow();
        String sparkPoolName = runContext.render(this.rSparkPoolName).as(String.class).orElseThrow();

        logger.debug("Connecting to Azure Synapse endpoint: {}, Spark pool: {}", endpoint, sparkPoolName);

        SparkBatchClient client = new SparkClientBuilder()
            .endpoint(endpoint)
            .sparkPoolName(sparkPoolName)
            .credential(this.credentials(runContext))
            .buildSparkBatchClient();

        logger.info("Successfully authenticated to Azure Synapse Spark");

        String jobName = runContext.render(this.rName).as(String.class).orElseThrow();
        String jobFile = runContext.render(this.rFile).as(String.class).orElseThrow();

        logger.debug("Configuring Spark batch job: name={}, file={}", jobName, jobFile);

        SparkBatchJobOptions options = new SparkBatchJobOptions()
            .setName(jobName)
            .setFile(jobFile);

        if (rClassName != null) {
            runContext.render(rClassName).as(String.class).ifPresent(options::setClassName);
        }
        if (rArguments != null) {
            List<String> argsList = runContext.render(rArguments).asList(String.class);
            if (!argsList.isEmpty()) {
                logger.debug("Adding {} arguments to job", argsList.size());
                options.setArguments(argsList);
            }
        }
        if (rJars != null) {
            List<String> jarsList = runContext.render(rJars).asList(String.class);
            if (!jarsList.isEmpty()) {
                logger.debug("Adding {} additional JARs", jarsList.size());
                options.setJars(jarsList);
            }
        }
        if (rPyFiles != null) {
            List<String> pyFilesList = runContext.render(rPyFiles).asList(String.class);
            if (!pyFilesList.isEmpty()) {
                logger.debug("Adding {} Python files", pyFilesList.size());
                options.setPythonFiles(pyFilesList);
            }
        }
        if (rFiles != null) {
            List<String> filesList = runContext.render(rFiles).asList(String.class);
            if (!filesList.isEmpty()) {
                logger.debug("Adding {} additional files", filesList.size());
                options.setFiles(filesList);
            }
        }
        if (rArchives != null) {
            List<String> archivesList = runContext.render(rArchives).asList(String.class);
            if (!archivesList.isEmpty()) {
                logger.debug("Adding {} archives", archivesList.size());
                options.setArchives(archivesList);
            }
        }
        if (rConf != null) {
            Map<String, String> confMap = runContext.render(rConf).asMap(String.class, String.class);
            if (!confMap.isEmpty()) {
                logger.debug("Applying {} Spark configuration properties", confMap.size());
                options.setConfiguration(confMap);
            }
        }
        if (rDriverMemory != null) {
            runContext.render(rDriverMemory).as(String.class).ifPresent(m -> {
                logger.debug("Setting driver memory: {}", m);
                options.setDriverMemory(m);
            });
        }
        if (rDriverCores != null) {
            runContext.render(rDriverCores).as(Integer.class).ifPresent(c -> {
                logger.debug("Setting driver cores: {}", c);
                options.setDriverCores(c);
            });
        }
        if (rExecutorMemory != null) {
            runContext.render(rExecutorMemory).as(String.class).ifPresent(m -> {
                logger.debug("Setting executor memory: {}", m);
                options.setExecutorMemory(m);
            });
        }
        if (rExecutorCores != null) {
            runContext.render(rExecutorCores).as(Integer.class).ifPresent(c -> {
                logger.debug("Setting executor cores: {}", c);
                options.setExecutorCores(c);
            });
        }
        if (rExecutorCount != null) {
            runContext.render(rExecutorCount).as(Integer.class).ifPresent(e -> {
                logger.debug("Setting executor count: {}", e);
                options.setExecutorCount(e);
            });
        }
        if (rTags != null) {
            Map<String, String> tagsMap = runContext.render(rTags).asMap(String.class, String.class);
            if (!tagsMap.isEmpty()) {
                logger.debug("Adding {} tags to job", tagsMap.size());
                options.setTags(tagsMap);
            }
        }

        SparkBatchJob job = client.createSparkBatchJob(options);
        logger.info("Created Spark batch job with ID: {}", job.getId());

        return Output.builder()
            .jobId(job.getId())
            .jobName(job.getName())
            .state(job.getState().toString())
            .appId(job.getAppId())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The Spark batch job ID."
        )
        private final Integer jobId;

        @Schema(
            title = "The Spark batch job name."
        )
        private final String jobName;

        @Schema(
            title = "The state of the Spark batch job.",
            description = "Possible states: not_started, starting, running, idle, busy, shutting_down, error, dead, killed, success"
        )
        private final String state;

        @Schema(
            title = "The Spark application ID."
        )
        private final String appId;
    }
}
