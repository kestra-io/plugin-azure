package io.kestra.plugin.azure.storage.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.specialized.DataLakeLeaseClient;
import com.azure.storage.file.datalake.specialized.DataLakeLeaseClientBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeWithFileName;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.kestra.plugin.azure.storage.adls.services.DataLakeService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;

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
                id: azure_storage_datalake_read
                namespace: company.team

                tasks:
                  - id: read_file
                    type: io.kestra.plugin.azure.storage.adls.Read
                    endpoint: "https://yourblob.blob.core.windows.net"
                    sasToken: "{{ secret('SAS_TOKEN') }}"
                    fileSystem: "mydata"
                    fileName: "path/to/myfile"
                    leaseDuration: 20
                    action: ACQUIRE
                """
        )
    }
)
@Schema(
    title = "Read a file from Azure Data Lake Storage."
)
public class Lease extends AbstractDataLakeWithFileName implements RunnableTask<Lease.Output> {

    @Schema(
        title = "Lease duration in seconds",
        description = "To be used with the action 'ACQUIRE'. The duration of the lease must be between 15 and 60 seconds or left blank for an infinite duration."
    )
    @NotNull
    @Builder.Default
    protected Property<Integer> leaseDuration = Property.of(-1); //Set to -1 for infinite lease duration by default

    @Schema(
        title = "Lease action",
        description = "The lease action you want to set (ex: 'ACQUIRE')"
    )
    @NotNull
    @Builder.Default
    protected Property<LeaseAction> action = Property.of(LeaseAction.ACQUIRE);

    @Schema(
        title = "Lease ID",
        description = "ID of the lease that must be provided for the following action : RENEW, BREAK, RELEASE"
    )
    protected Property<String> leaseId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        DataLakeFileClient client = this.dataLakeFileClient(runContext);

        String resultLeaseId = runContext.render(leaseId).as(String.class).orElse(null);
        final LeaseAction renderedAction = runContext.render(action).as(LeaseAction.class).orElseThrow();

        DataLakeLeaseClientBuilder dataLakeLeaseClientBuilder = new DataLakeLeaseClientBuilder()
            .fileClient(client);

        if(!renderedAction.equals(LeaseAction.ACQUIRE)) {
            if(resultLeaseId == null) {
                throw new IllegalArgumentException("Lease ID must be provided for action RENEW, BREAK or RELEASE.");
            }
            dataLakeLeaseClientBuilder.leaseId(resultLeaseId);
        }

        DataLakeLeaseClient dataLakeLeaseClient = dataLakeLeaseClientBuilder.buildClient();

        switch (renderedAction) {
            case ACQUIRE -> resultLeaseId = dataLakeLeaseClient.acquireLease(runContext.render(leaseDuration).as(Integer.class).orElseThrow());
            case RENEW -> resultLeaseId = dataLakeLeaseClient.renewLease();
            case RELEASE -> dataLakeLeaseClient.releaseLease();
            case BREAK -> dataLakeLeaseClient.breakLease();
        }

        return Output
            .builder()
            .id(resultLeaseId)
            .build();
    }

    public enum LeaseAction {
        ACQUIRE,
        RENEW,
        RELEASE,
        BREAK
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Lease ID."
        )
        private final String id;
    }
}
