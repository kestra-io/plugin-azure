package io.kestra.plugin.azure.cli;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
public class AzCLITest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        String envKey = "MY_KEY";
        String envValue = "MY_VALUE";

        AzCLI execute = AzCLI.builder()
                .id(IdUtils.create())
                .type(AzCLI.class.getName())
                .env(Map.of("{{ inputs.envKey }}", "{{ inputs.envValue }}"))
                .commands(List.of(
                        "echo \"::{\\\"outputs\\\":{\\\"{{ inputs.outputName }}\\\":\\\"$" + envKey + "\\\"}}::\""
                ))
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
                .username("{{ inputs.myUser }}")
                .password("{{ inputs.myPassword }}")
                .tenant("{{ inputs.myTenant }}")
                .servicePrincipal(true)
                .build();

        String username = "someUser";
        String password = "somePassword";
        String tenant = "someTenant";
        runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of(
                "myUser", username,
                "myPassword", password,
                "myTenant", tenant
        ));
        assertThat(execute.getLoginCommands(runContext), allOf(
                iterableWithSize(1),
                hasItem("az login -u " + username +
                        " -p " + password +
                        " --tenant " + tenant +
                        " --service-principal")
        ));
    }
}
