package io.kestra.plugin.azure.batch.job;

import com.google.common.io.CharStreams;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.azure.batch.models.*;
import io.kestra.plugin.azure.storage.blob.SharedAccess;
import io.kestra.plugin.azure.storage.blob.Upload;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
class CreateTest extends AbstractTest {
    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    SharedAccess.Output sas(String container, String name, SharedAccess.Permission perms) throws Exception {
        SharedAccess task = SharedAccess.builder()
            .id(SharedAccess.class.getSimpleName())
            .type(io.kestra.plugin.azure.storage.blob.List.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(container))
            .name(Property.ofValue(name))
            .expirationDate(Property.ofExpression("{{ now() | dateAdd(1, 'DAYS')  }}"))
            .permissions(Set.of(perms))
            .build();
        return task.run(runContext(task));
    }

    URI uploadToContainer(String content) throws Exception {
        String prefix = IdUtils.create();

        Upload upload = Upload.builder()
            .id(CreateTest.class.getSimpleName())
            .type(Upload.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .from(Property.ofValue(upload(content.getBytes(StandardCharsets.UTF_8)).toString()))
            .name(Property.ofValue("batch/" + prefix + ".yml"))
            .build();

        Upload.Output uploadRun = upload.run(runContext(upload));

        return sas(uploadRun.getBlob().getContainer(), uploadRun.getBlob().getName(), SharedAccess.Permission.READ).getUri();
    }

    private Create.Output create(List<Task> tasks, Map<String, Object> inputs) throws Exception {
        Create task = Create.builder()
            .id(CreateTest.class.getSimpleName())
            .type(Create.class.getName())
            .endpoint(Property.ofValue(this.endpoint))
            .account(Property.ofValue(this.account))
            .accessKey(Property.ofValue(this.accessKey))
            .poolId(Property.ofValue(this.poolId))
            .job(Job.builder()
                .id(IdUtils.create())
                .build()
            )
            .tasks(tasks)
            .build();

        return task.run(runContext(task, inputs));
    }

    @Test
    @Disabled("pool are not running !")
    void run() throws Exception {
        Flux<LogEntry> receive = TestsUtils.receive(logQueue);

        String random = IdUtils.create();
        SharedAccess.Output outputs = sas(this.container, "files/out/", SharedAccess.Permission.WRITE);

        Create.Output run = create(
            List.of(
                Task.builder()
                    .id("env")
                    .interpreter(Property.ofValue("/bin/bash"))
                    .commands(Property.ofValue(List.of("echo t1=$ENV_STRING | awk '{ print $1 }'")))
                    .environments(Property.ofValue(Map.of("ENV_STRING", "{{ inputs.first }}")))
                    .containerSettings(TaskContainerSettings.builder().imageName(Property.ofValue("ubuntu")).build())
                    .build(),
                Task.builder()
                    .id("echo")
                    .interpreter(Property.ofValue("/bin/bash"))
                    .commands(Property.ofValue(List.of("echo t2=`echo {{ inputs.second }}` 1>&2")))
                    .containerSettings(TaskContainerSettings.builder().imageName(Property.ofValue("ubuntu")).build())
                    .build(),
                Task.builder()
                    .id("for")
                    .interpreter(Property.ofValue("/bin/bash"))
                    .commands(Property.ofValue(List.of(("for i in $(seq 10); do echo t3=$i; done"))))
                    .containerSettings(TaskContainerSettings.builder().imageName(Property.ofValue("ubuntu")).build())
                    .build(),
                Task.builder()
                    .id("vars")
                    .resourceFiles(List.of(
                        ResourceFile.builder()
                            .filePath(Property.ofValue("files/in/in.txt"))
                            .httpUrl(Property.ofValue(uploadToContainer(random).toString()))
                            .build()
                    ))
                    .uploadFiles(List.of(
                        OutputFile.builder()
                            .filePattern(Property.ofValue("files/in/*"))
                            .destination(OutputFileDestination.builder()
                                .container(OutputFileBlobContainerDestination.builder()
                                    .containerUrl(Property.ofValue(outputs.getUri().toString()))
                                    .build()
                                )
                                .build()
                            )
                            .build()
                    ))
                    .interpreter(Property.ofValue("/bin/bash"))
                    .commands(Property.ofValue(List.of("echo '::{\"outputs\": {\"extract\":\"'$(cat files/in/in.txt)'\"}}::' | tee files/in/tee.txt")))
                    .containerSettings(TaskContainerSettings.builder().imageName(Property.ofValue("ubuntu")).build())
                    .build(),
                Task.builder()
                    .id("output")
                    .outputFiles(Property.ofValue(List.of(
                        "outs/1.txt"
                    )))
                    .outputDirs(Property.ofValue(List.of(
                        "outs/child"
                    )))
                    .interpreter(Property.ofValue("/bin/bash"))
                    .commands(Property.ofValue(List.of(
                        "mkdir -p outs/child/sub",
                        "echo 1 > outs/1.txt",
                        "echo 2 > outs/child/2.txt",
                        "echo 3 > outs/child/sub/3.txt"
                    )))
                    .containerSettings(TaskContainerSettings.builder().imageName(Property.ofValue("ubuntu")).build())
                    .build()
            ),
            Map.of("first", "first", "second", "second")
        );

        Thread.sleep(100);

        assertThat(run.getVars().get("extract"), is(random));
        List<LogEntry> logs = receive.collectList().block();
        assertThat(logs.stream().filter(logEntry -> logEntry.getMessage().equals("t1=first")).count(), is(1L));
        assertThat(logs.stream().filter(logEntry -> logEntry.getMessage().equals("t2=second")).filter(logEntry -> logEntry.getLevel().equals(Level.ERROR)).count(), is(1L));
        assertThat(logs.stream().filter(logEntry -> logEntry.getMessage().equals("t3=5")).count(), is(1L));

        InputStream get = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("outs/1.txt"));
        assertThat(CharStreams.toString(new InputStreamReader(get)), is("1\n"));

        get = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("2.txt"));
        assertThat(CharStreams.toString(new InputStreamReader(get)), is("2\n"));

        get = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("sub/3.txt"));
        assertThat(CharStreams.toString(new InputStreamReader(get)), is("3\n"));
    }

    @Disabled("pool are not running !")
    @Test
    void errors() throws Exception {
        Flux<LogEntry> receive = TestsUtils.receive(logQueue);

        Exception exception = assertThrows(Exception.class, () -> create(
            List.of(
                Task.builder()
                    .id("echo")
                    .commands(Property.ofValue(List.of(("echo ok"))))
                    .containerSettings(TaskContainerSettings.builder().imageName(Property.ofValue("ubuntu")).build())
                    .build(),
                Task.builder()
                    .id("failed")
                    .commands(Property.ofValue(List.of(("cat failed"))))
                    .containerSettings(TaskContainerSettings.builder().imageName(Property.ofValue("ubuntu")).build())
                    .build()
            ),
            Map.of()
        ));
        Thread.sleep(100);

        assertThat(exception.getMessage(), containsString("1/2 task(s) failed"));
        assertThat(receive.collectList().block().stream().filter(logEntry -> logEntry.getMessage().equals("ok")).count(), is(1L));
    }
}
