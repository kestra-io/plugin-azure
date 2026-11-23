package io.kestra.plugin.azure.cli;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.runner.docker.Docker;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class AzCLIExecScriptTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void defaultsAreUnchanged() {
        var task = AzCLI.builder()
            .id(IdUtils.create())
            .type(AzCLI.class.getName())
            .commands(Property.ofValue(List.of("az --help")))
            .build();

        assertThat(task, instanceOf(AbstractExecScript.class));
        assertThat(task.getTaskRunner(), instanceOf(Docker.class));
        assertThat(task.getContainerImage(), is(Property.ofValue("mcr.microsoft.com/azure-cli")));
        assertThat(task.getEnv(), nullValue());
        assertThat(task.getOutputFiles(), nullValue());
    }

    @Test
    void beforeCommandsRunAfterLogin() throws Exception {
        var dir = Files.createTempDirectory(Files.createDirectories(Path.of("build", "azcli-test").toAbsolutePath()), "run");
        var trace = dir.resolve("trace.txt");
        var fakeAz = dir.resolve("az");
        Files.writeString(fakeAz, "#!/bin/sh\necho login >> \"$TRACE\"\n");
        Files.setPosixFilePermissions(fakeAz, PosixFilePermissions.fromString("rwxr-xr-x"));

        var task = AzCLI.builder()
            .id(IdUtils.create())
            .type(AzCLI.class.getName())
            .taskRunner(Process.instance())
            .env(
                Property.ofValue(
                    Map.of(
                        "TRACE", trace.toString(),
                        "PATH", dir + ":" + System.getenv("PATH")
                    )
                )
            )
            .username(Property.ofValue("user"))
            .beforeCommands(Property.ofValue(List.of("echo before >> \"$TRACE\"")))
            .commands(
                Property.ofValue(
                    List.of(
                        "echo \"::{\\\"outputs\\\":{\\\"order\\\":\\\"$(tr '\\n' ',' < $TRACE)\\\"}}::\""
                    )
                )
            )
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        assertThat(output.getVars().get("order"), is("login,before,"));
    }

    @Test
    void beforeCommandsWithoutLogin() throws Exception {
        var dir = Files.createTempDirectory(Files.createDirectories(Path.of("build", "azcli-test").toAbsolutePath()), "run");
        var trace = dir.resolve("trace.txt");

        var task = AzCLI.builder()
            .id(IdUtils.create())
            .type(AzCLI.class.getName())
            .taskRunner(Process.instance())
            .env(Property.ofValue(Map.of("TRACE", trace.toString())))
            .beforeCommands(Property.ofValue(List.of("echo before >> \"$TRACE\"")))
            .commands(
                Property.ofValue(
                    List.of(
                        "echo \"::{\\\"outputs\\\":{\\\"order\\\":\\\"$(tr '\\n' ',' < $TRACE)\\\"}}::\""
                    )
                )
            )
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.getVars().get("order"), is("before,"));
    }
}
