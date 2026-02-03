package io.kestra.plugin.azure.cli;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.AllOf.allOf;

@KestraTest(environments = "sp")
public class AzCLITest {
    @Value("${kestra.variables.globals.azure.sp.username}")
    protected String username;
    @Value("${kestra.variables.globals.azure.sp.secret}")
    protected String secret;
    @Value("${kestra.variables.globals.azure.sp.tenant}")
    String tenant;

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        String envKey = "MY_KEY";
        String envValue = "MY_VALUE";

        AzCLI execute = AzCLI.builder()
            .id(IdUtils.create())
            .type(AzCLI.class.getName())
            .env(Property.ofValue(Map.of(envKey, envValue)))
            .commands(TestsUtils.propertyFromList(List.of(
                "echo \"::{\\\"outputs\\\":{\\\"{{ inputs.outputName }}\\\":\\\"$" + envKey + "\\\"}}::\""
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of(
            "envKey", envKey,
            "envValue", envValue,
            "outputName", "customEnv"
        ));

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(runOutput.getVars().get("customEnv"), is(envValue));
        assertThat(execute.getLoginCommands(runContext), empty());

        execute = AzCLI.builder()
            .id(IdUtils.create())
            .type(AzCLI.class.getName())
            .username(Property.ofExpression("{{ inputs.myUser }}"))
            .password(Property.ofExpression("{{ inputs.myPassword }}"))
            .tenant(Property.ofExpression("{{ inputs.myTenant }}"))
            .servicePrincipal(Property.ofValue(true))
            .commands(Property.ofValue(List.of("az keyvault list")))
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of(
            "myUser", username,
            "myPassword", secret,
            "myTenant", tenant
        ));
        assertThat(execute.getLoginCommands(runContext), allOf(
            iterableWithSize(1),
            hasItem("az login -u " + username +
                " -p " + secret +
                " --tenant " + tenant +
                " --service-principal")
        ));

        runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));

    }
}
