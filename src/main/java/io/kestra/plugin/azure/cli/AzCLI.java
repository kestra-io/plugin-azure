package io.kestra.plugin.azure.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.exec.scripts.services.ScriptService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Execute one or more `az` commands from a Command Line Interface. We recommend using a Service Principal and a Client Secret for authentication. To create a Service Principal and Client Secret, you can use the following [documentation](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/guides/service_principal_client_secret). Then, use the generated `appId` as the `username` and the generated `password` as the `password` in the Kestra task configuration. Finally, pass the returned `tenant` ID to the `tenant` field in the Kestra task configuration and set `servicePrincipal` to true.",
)
@Plugin(
        examples = {
                @Example(
                        title = "List ActiveDirectory users for the currently used tenant",
                        code = {
                                "username: \"<appId>\"",
                                "password: \"{{secret('AZURE_SERVICE_PRINCIPAL_PASSWORD')}}\"",
                                "tenant: \"{{secret('AZURE_TENANT_ID')}}\"",
                                "commands:",
                                "  - az ad user list"
                        }
                ),
                @Example(
                        title = "List all tenant's VMs successfully provisioned using a service principal authentication",
                        code = {
                                "username: \"<app-id>\"",
                                "password: \"secret('az-sp-pass-or-cert')\"",
                                "tenant: \"<tenant-id>\"",
                                "servicePrincipal: true",
                                "commands:",
                                "  - az vm list --query \"[?provisioningState=='Succeeded']\""
                        }
                ),
                @Example(
                        title = "Command without authentication",
                        code = {
                                "commands:",
                                "  - az --help"
                        }
                ),
                @Example(
                        full = true,
                        title = "List supported regions for the current Azure subscription",
                        code = """
                        id: azureRegions
                        namespace: dev
                        tasks:
                          - id: svc-principal
                            type: io.kestra.plugin.azure.cli.AzCLI
                            tenant: {{secret('AZURE_TENANT_ID')}}
                            username: {{secret('AZURE_SERVICE_PRINCIPAL_CLIENT_ID')}}
                            password: {{secret('AZURE_SERVICE_PRINCIPAL_PASSWORD')}}
                            servicePrincipal: true
                            commands:
                              - az account list-locations --query "[].{Region:name}" -o table"""
                )                
        }
)
public class AzCLI extends Task implements RunnableTask<ScriptOutput> {
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
            title = "Tenant id to use."
    )
    @PluginProperty(dynamic = true)
    private String tenant;

    @Schema(
            title = "Is the account a service principal ?"
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
            title = "Docker options for the `DOCKER` runner."
    )
    @PluginProperty
    @Builder.Default
    protected DockerOptions docker = DockerOptions.builder()
            .image("mcr.microsoft.com/azure-cli")
            .build();

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        List<String> loginCommands = this.getLoginCommands(runContext);

        CommandsWrapper commands = new CommandsWrapper(runContext)
                .withWarningOnStdErr(true)
                .withRunnerType(RunnerType.DOCKER)
                .withDockerOptions(this.docker)
                .withCommands(
                        ScriptService.scriptCommands(
                                List.of("/bin/sh", "-c"),
                                loginCommands,
                                this.commands)
                );

        commands = commands.withEnv(this.env);

        return commands.run();
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