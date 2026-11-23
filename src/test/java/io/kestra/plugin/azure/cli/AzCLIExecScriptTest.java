package io.kestra.plugin.azure.cli;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.runner.docker.Docker;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    void failingLoginStopsCommandsByDefault() throws Exception {
        var dir = setupFailingAz();

        var task = failingAzTask(dir, null);
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var exception = assertThrows(RunnableTaskException.class, () -> task.run(runContext));
        assertThat(exception.getOutput(), instanceOf(ScriptOutput.class));
        assertThat(((ScriptOutput) exception.getOutput()).getExitCode(), is(1));
        assertThat(Files.readString(dir.resolve("trace.txt")), is("login\n"));
        assertThat(Files.exists(dir.resolve("ran.txt")), is(false));
    }

    @Test
    void failingLoginContinuesWhenFailFastDisabled() throws Exception {
        var dir = setupFailingAz();

        var task = failingAzTask(dir, Property.ofValue(false));
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        task.run(runContext);

        assertThat(Files.readString(dir.resolve("trace.txt")), is("login\n"));
        assertThat(Files.exists(dir.resolve("ran.txt")), is(true));
    }

    private Path setupFailingAz() throws Exception {
        var dir = Files.createTempDirectory(Files.createDirectories(Path.of("build", "azcli-test").toAbsolutePath()), "run");
        var fakeAz = dir.resolve("az");
        Files.writeString(fakeAz, "#!/bin/sh\necho login >> \"$(dirname \"$0\")/trace.txt\"\nexit 1\n");
        Files.setPosixFilePermissions(fakeAz, PosixFilePermissions.fromString("rwxr-xr-x"));
        return dir;
    }

    private AzCLI failingAzTask(Path dir, Property<Boolean> failFast) {
        var builder = AzCLI.builder()
            .id(IdUtils.create())
            .type(AzCLI.class.getName())
            .taskRunner(Process.instance())
            .env(Property.ofValue(Map.of("PATH", dir + ":" + System.getenv("PATH"))))
            .username(Property.ofValue("user"))
            .commands(Property.ofValue(List.of("echo ran > " + dir.resolve("ran.txt"))));
        if (failFast != null) {
            builder.failFast(failFast);
        }
        return builder.build();
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
