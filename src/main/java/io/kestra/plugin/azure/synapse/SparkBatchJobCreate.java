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
    title = "Submit Spark batch to Synapse pool",
    description = "Creates and submits a Spark batch job to an Azure Synapse Analytics Spark pool using the workspace endpoint, pool name, and optional resource overrides. Requires valid Azure credentials with permission to submit Spark jobs."
)
public class SparkBatchJobCreate extends AbstractAzureIdentityConnection implements RunnableTask<SparkBatchJobCreate.Output> {

    @Schema(
        title = "Synapse workspace endpoint",
        description = "Workspace URL in the form `https://{workspace}.dev.azuresynapse.net`."
    )
    @NotNull
    private Property<String> endpoint;

    @Schema(
        title = "Spark pool name",
        description = "Name of the Spark pool where the batch job will be submitted."
    )
    @NotNull
    private Property<String> sparkPoolName;

    @Schema(
        title = "Spark batch job name"
    )
    @NotNull
    private Property<String> name;

    @Schema(
        title = "Main application file",
        description = "Path to the main application file (JAR, Python, etc.) in ADLS Gen2 storage in the form `abfss://{container}@{storage-account}.dfs.core.windows.net/path/to/file`."
    )
    @NotNull
    private Property<String> file;

    @Schema(
        title = "Main class name",
        description = "Fully qualified class name for Java/Scala Spark jobs."
    )
    private Property<String> className;

    @Schema(
        title = "Command line arguments",
        description = "Arguments passed to the main method."
    )
    private Property<List<String>> arguments;

    @Schema(
        title = "Additional JAR files",
        description = "Additional JARs to be used in the job."
    )
    private Property<List<String>> jars;

    @Schema(
        title = "Additional Python files",
        description = "Additional Python files to be used in the job."
    )
    private Property<List<String>> pyFiles;

    @Schema(
        title = "Additional files",
        description = "Additional files to be used in the job."
    )
    private Property<List<String>> files;

    @Schema(
        title = "Archives",
        description = "Archives to be used in the job."
    )
    private Property<List<String>> archives;

    @Schema(
        title = "Spark configuration",
        description = "Spark configuration properties for the job."
    )
    private Property<Map<String, String>> conf;

    @Schema(
        title = "Driver memory",
        description = "Memory for the driver process (e.g., `28g`)."
    )
    private Property<String> driverMemory;

    @Schema(
        title = "Driver cores",
        description = "Number of cores for the driver process."
    )
    private Property<Integer> driverCores;

    @Schema(
        title = "Executor memory",
        description = "Memory per executor process (e.g., `28g`)."
    )
    private Property<String> executorMemory;

    @Schema(
        title = "Executor cores",
        description = "Number of cores per executor."
    )
    private Property<Integer> executorCores;

    @Schema(
        title = "Executor count",
        description = "Number of executor processes to launch."
    )
    private Property<Integer> executorCount;

    @Schema(
        title = "Tags",
        description = "Tags to associate with the job."
    )
    private Property<Map<String, String>> tags;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rEndpoint = runContext.render(this.endpoint).as(String.class).orElseThrow();
        String rSparkPoolName = runContext.render(this.sparkPoolName).as(String.class).orElseThrow();

        SparkBatchClient client = new SparkClientBuilder()
            .endpoint(rEndpoint)
            .sparkPoolName(rSparkPoolName)
            .credential(this.credentials(runContext))
            .buildSparkBatchClient();

        SparkBatchJobOptions options = new SparkBatchJobOptions()
            .setName(runContext.render(this.name).as(String.class).orElseThrow())
            .setFile(runContext.render(this.file).as(String.class).orElseThrow());

        if (className != null) {
            runContext.render(className).as(String.class).ifPresent(options::setClassName);
        }
        if (arguments != null) {
            List<String> rArguments = runContext.render(arguments).asList(String.class);
            if (!rArguments.isEmpty()) {
                options.setArguments(rArguments);
            }
        }
        if (jars != null) {
            List<String> rJars = runContext.render(jars).asList(String.class);
            if (!rJars.isEmpty()) {
                options.setJars(rJars);
            }
        }
        if (pyFiles != null) {
            List<String> rPyFiles = runContext.render(pyFiles).asList(String.class);
            if (!rPyFiles.isEmpty()) {
                options.setPythonFiles(rPyFiles);
            }
        }
        if (files != null) {
            List<String> rFiles = runContext.render(files).asList(String.class);
            if (!rFiles.isEmpty()) {
                options.setFiles(rFiles);
            }
        }
        if (archives != null) {
            List<String> rArchives = runContext.render(archives).asList(String.class);
            if (!rArchives.isEmpty()) {
                options.setArchives(rArchives);
            }
        }
        if (conf != null) {
            Map<String, String> rConf = runContext.render(conf).asMap(String.class, String.class);
            if (!rConf.isEmpty()) {
                options.setConfiguration(rConf);
            }
        }
        if (driverMemory != null) {
            runContext.render(driverMemory).as(String.class).ifPresent(options::setDriverMemory);
        }
        if (driverCores != null) {
            runContext.render(driverCores).as(Integer.class).ifPresent(options::setDriverCores);
        }
        if (executorMemory != null) {
            runContext.render(executorMemory).as(String.class).ifPresent(options::setExecutorMemory);
        }
        if (executorCores != null) {
            runContext.render(executorCores).as(Integer.class).ifPresent(options::setExecutorCores);
        }
        if (executorCount != null) {
            runContext.render(executorCount).as(Integer.class).ifPresent(options::setExecutorCount);
        }
        if (tags != null) {
            Map<String, String> rTags = runContext.render(tags).asMap(String.class, String.class);
            if (!rTags.isEmpty()) {
                options.setTags(rTags);
            }
        }

        SparkBatchJob job = client.createSparkBatchJob(options);
        logger.info("Submitted Spark batch job '{}' with ID {}", job.getName(), job.getId());

        return Output.builder()
            .jobId(job.getId())
            .state(job.getState().toString())
            .appId(job.getAppId())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Spark batch job ID"
        )
        private final Integer jobId;

        @Schema(
            title = "Spark batch job state",
            description = "Possible states: `not_started`, `starting`, `running`, `idle`, `busy`, `shutting_down`, `error`, `dead`, `killed`, `success`."
        )
        private final String state;

        @Schema(
            title = "Spark application ID"
        )
        private final String appId;
    }
}
