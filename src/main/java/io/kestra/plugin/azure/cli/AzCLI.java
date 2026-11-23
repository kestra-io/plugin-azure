package io.kestra.plugin.azure.cli;

import java.util.ArrayList;
import java.util.List;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.runner.docker.Docker;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
public class AzCLI extends AbstractExecScript implements RunnableTask<ScriptOutput> {
    private static final String DEFAULT_IMAGE = "mcr.microsoft.com/azure-cli";

    @Schema(
        title = "Commands",
        description = """
            List of az commands executed with the configured interpreter, `/bin/sh -c` by default, inside the runner. \
            Commands stop at the first failing command unless `failFast` is set to `false`."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<String>> commands;

    @Schema(title = "Login username", description = "Triggers az login before commands; for Service Principal use appId/clientId")
    @PluginProperty(group = "connection")
    private Property<String> username;

    @Schema(title = "Login password", description = "Password or client secret used with username")
    @PluginProperty(group = "connection", secret = true)
    private Property<String> password;

    @Schema(title = "Tenant ID", description = "Tenant passed to az login --tenant")
    @PluginProperty(group = "connection")
    private Property<String> tenant;

    @Schema(title = "Service principal login", description = "Adds --service-principal to az login when true")
    @PluginProperty(group = "advanced")
    private Property<Boolean> servicePrincipal;

    @Schema(
        title = "Deprecated Docker options",
        description = "Deprecated; use taskRunner instead"
    )
    @PluginProperty(group = "deprecated")
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "Task runner",
        description = "Runner implementation (defaults to Docker) used to execute the commands"
    )
    @PluginProperty(group = "execution")
    @Builder.Default
    @Valid
    protected TaskRunner<?> taskRunner = Docker.instance();

    @Schema(title = "Runner container image", description = "Container image used by container-based task runners; defaults to mcr.microsoft.com/azure-cli")
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        var beforeCommands = new ArrayList<>(this.getLoginCommands(runContext));
        beforeCommands.addAll(runContext.render(this.getBeforeCommands()).asList(String.class));

        return this.commands(runContext)
            .withDockerOptions(injectDefaults(getDocker()))
            .withWarningOnStdErr(runContext.render(this.getWarningOnStdErr()).as(Boolean.class).orElse(true))
            .withInterpreter(this.getInterpreter())
            .withBeforeCommands(Property.ofValue(beforeCommands))
            .withBeforeCommandsWithOptions(true)
            .withCommands(this.commands)
            .run();
    }

    @Override
    protected DockerOptions injectDefaults(DockerOptions original) {
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
