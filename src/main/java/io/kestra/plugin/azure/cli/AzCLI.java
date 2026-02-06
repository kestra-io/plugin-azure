package io.kestra.plugin.azure.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run Azure CLI commands in a container",
    description = "Executes one or more az commands inside the task runner (defaults to Docker with image mcr.microsoft.com/azure-cli). If username is set, performs az login with optional password/client secret, tenant, and --service-principal flag. Prefer taskRunner over deprecated docker options."
)
@Plugin(
    examples = {
        @Example(
            title = "List Azure Active Directory users for the currently authenticated tenant.",
            full = true,
            code = """
                id: azure_cli
                namespace: company.team

                tasks:
                  - id: az_cli
                    type: io.kestra.plugin.azure.cli.AzCLI
                    username: "azure_app_id"
                    password: "{{ secret('AZURE_SERVICE_PRINCIPAL_PASSWORD') }}"
                    tenant: "{{ secret('AZURE_TENANT_ID') }}"
                    commands:
                      - az ad user list
                """
        ),
        @Example(
            title = "List all successfully provisioned VMs using a Service Principal authentication.",
            full = true,
            code = """
                id: azure_cli
                namespace: company.team

                tasks:
                  - id: az_cli
                    type: io.kestra.plugin.azure.cli.AzCLI
                    username: "azure_app_id"
                    password: "{{ secret('AZURE_SERVICE_PRINCIPAL_PASSWORD') }}"
                    tenant: "{{ secret('AZURE_TENANT_ID') }}"
                    servicePrincipal: true
                    commands:
                      - az vm list --query "[?provisioningState=='Succeeded']"
                """
        ),
        @Example(
            title = "Command without authentication.",
            full = true,
            code = """
                id: azure_cli
                namespace: company.team

                tasks:
                  - id: az_cli
                    type: io.kestra.plugin.azure.cli.AzCLI
                    commands:
                      - az --help
                """
        ),
        @Example(
            full = true,
            title = "List supported regions for the current Azure subscription.",
            code = """
                id: azure_cli
                namespace: company.team

                tasks:
                  - id: list_locations
                    type: io.kestra.plugin.azure.cli.AzCLI
                    tenant: "{{ secret('AZURE_TENANT_ID') }}"
                    username: "{{ secret('AZURE_SERVICE_PRINCIPAL_CLIENT_ID') }}"
                    password: "{{ secret('AZURE_SERVICE_PRINCIPAL_PASSWORD') }}"
                    servicePrincipal: true
                    commands:
                      - az account list-locations --query "[].{Region:name}" -o table"""
        )
    }
)
public class AzCLI extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {
    private static final String DEFAULT_IMAGE = "mcr.microsoft.com/azure-cli";

    @Schema(title = "Commands", description = "List of az commands executed with /bin/sh -c inside the runner")
    @NotNull
    private Property<List<String>> commands;

    @Schema(title = "Login username", description = "Triggers az login before commands; for Service Principal use appId/clientId")
    private Property<String> username;

    @Schema(title = "Login password", description = "Password or client secret used with username")
    private Property<String> password;

    @Schema(title = "Tenant ID", description = "Tenant passed to az login --tenant")
    private Property<String> tenant;

    @Schema(title = "Service principal login", description = "Adds --service-principal to az login when true")
    private Property<Boolean> servicePrincipal;

    @Schema(title = "Environment variables", description = "Additional environment variables injected into the commands")
    protected Property<Map<String, String>> env;

    @Schema(
        title = "Deprecated Docker options",
        description = "Deprecated; use taskRunner instead"
    )
    @PluginProperty
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "Task runner",
        description = "Runner implementation (defaults to Docker) used to execute the commands"
    )
    @PluginProperty
    @Builder.Default
    @Valid
    protected TaskRunner<?> taskRunner = Docker.instance();

    @Schema(title = "Runner container image", description = "Container image used by container-based task runners; defaults to mcr.microsoft.com/azure-cli")
    @Builder.Default
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    private NamespaceFiles namespaceFiles;

    private Object inputFiles;

    @Schema(title = "Output files", description = "Paths from the container working directory to persist to outputs")
    private Property<List<String>> outputFiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        List<String> loginCommands = this.getLoginCommands(runContext);

        var renderedEnv = runContext.render(this.env).asMap(String.class, String.class);
        var renderedOutputFiles = runContext.render(this.outputFiles).asList(String.class);

        CommandsWrapper commands = new CommandsWrapper(runContext)
            .withWarningOnStdErr(true)
            .withDockerOptions(injectDefaults(getDocker()))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(runContext.render(this.containerImage).as(String.class).orElseThrow())
            .withBeforeCommands(Property.ofValue(loginCommands))
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")))
            .withCommands(this.commands)
            .withEnv(renderedEnv.isEmpty() ? null : renderedEnv)
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(renderedOutputFiles.isEmpty() ? null : renderedOutputFiles);

        return commands.run();
    }

    private DockerOptions injectDefaults(DockerOptions original) {
        if (original == null) {
            return null;
        }

        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }

        return builder.build();
    }

    List<String> getLoginCommands(RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> loginCommands = new ArrayList<>();
        if (this.username != null) {
            StringBuilder loginCommand = new StringBuilder("az login -u ").append(runContext.render(this.username).as(String.class).orElseThrow());

            if (this.password != null) {
                loginCommand.append(" -p ").append(runContext.render(this.password).as(String.class).orElseThrow());
            }
            if (this.tenant != null) {
                loginCommand.append(" --tenant ").append(runContext.render(this.tenant).as(String.class).orElseThrow());
            }
            if (runContext.render(this.getServicePrincipal()).as(Boolean.class).orElse(false)) {
                loginCommand.append(" --service-principal");
            }

            loginCommands.add(loginCommand.toString());
        }
        return loginCommands;
    }
}
