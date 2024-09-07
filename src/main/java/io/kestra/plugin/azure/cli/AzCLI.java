package io.kestra.plugin.azure.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
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
    title = "Execute one or more `az` commands from a Command Line Interface. We recommend using a Service Principal and a Client Secret for authentication. " +
        "To create a Service Principal and Client Secret, you can use the following " +
        "[documentation](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/guides/service_principal_client_secret). " +
        "Then, use the generated `appId` as the `username` and the generated `password` as the `password` in the Kestra task configuration. " +
        "Finally, pass the returned `tenantId` to the `tenant` field in the Kestra task configuration and set `servicePrincipal` to `true`."
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

    @Schema(
        title = "The commands to run."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    private List<String> commands;

    @Schema(
        title = "Account username. If set, it will use `az login` before running the commands."
    )
    @PluginProperty(dynamic = true)
    private String username;

    @Schema(
        title = "Account password."
    )
    @PluginProperty(dynamic = true)
    private String password;

    @Schema(
        title = "Tenant ID to use."
    )
    @PluginProperty(dynamic = true)
    private String tenant;

    @Schema(
        title = "Is the account a service principal?"
    )
    @PluginProperty
    private boolean servicePrincipal;

    @Schema(
        title = "Additional environment variables for the current process."
    )
    @PluginProperty(
            additionalProperties = String.class,
            dynamic = true
    )
    protected Map<String, String> env;

    @Schema(
        title = "Deprecated, use 'taskRunner' instead"
    )
    @PluginProperty
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "The task runner to use.",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    protected TaskRunner taskRunner = Docker.instance();

    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
    @PluginProperty(dynamic = true)
    @Builder.Default
    protected String containerImage = DEFAULT_IMAGE;

    private NamespaceFiles namespaceFiles;

    private Object inputFiles;

    private List<String> outputFiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        List<String> loginCommands = this.getLoginCommands(runContext);

        CommandsWrapper commands = new CommandsWrapper(runContext)
            .withWarningOnStdErr(true)
            .withDockerOptions(injectDefaults(getDocker()))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(this.containerImage)
            .withCommands(
                ScriptService.scriptCommands(
                    List.of("/bin/sh", "-c"),
                    loginCommands,
                    this.commands)
            )
            .withEnv(this.env)
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(outputFiles);

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
            StringBuilder loginCommand = new StringBuilder("az login -u ").append(runContext.render(this.username));

            if (this.password != null) {
                loginCommand.append(" -p ").append(runContext.render(this.password));
            }
            if (this.tenant != null) {
                loginCommand.append(" --tenant ").append(runContext.render(this.tenant));
            }
            if (this.isServicePrincipal()) {
                loginCommand.append(" --service-principal");
            }

            loginCommands.add(loginCommand.toString());
        }
        return loginCommands;
    }
}
