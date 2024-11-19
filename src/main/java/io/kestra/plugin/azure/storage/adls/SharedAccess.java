package io.kestra.plugin.azure.storage.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.sas.DataLakeServiceSasSignatureValues;
import com.azure.storage.file.datalake.sas.PathSasPermission;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeWithFileName;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.stream.Collectors;

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
                id: azure_storage_adls_shared_access
                namespace: company.team

                tasks:
                  - id: shared_access
                    type: io.kestra.plugin.azure.storage.adls.SharedAccess
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
                    fileSystem: "mydata"
                    fileName: "path/to/my/file.txt"
                    expirationDate: "{{ now() | dateAdd(1, 'DAYS') }}"
                    permissions:
                      - r
                """
        )
    }
)
@Schema(
    title = "Shared Access on the Azure Data Lake Storage."
)
public class SharedAccess extends AbstractDataLakeWithFileName implements RunnableTask<SharedAccess.Output> {

    @Schema(
        title = " The time after which the SAS will no longer work."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String expirationDate;

    @Schema(
        title = " The permissions to be set for the Shared Access."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Set<Permission> permissions;

    @Schema(
        title = " The services to be set for the Shared Access."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Set<Service> services;

    @Schema(
        title = " The resource types to be set for the Shared Access."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Set<ResourceType> resourceTypes;

    @Override
    public Output run(RunContext runContext) throws Exception {
        DataLakeFileClient dataLakeServiceClient = this.dataLakeFileClient(runContext);

        OffsetDateTime offsetDateTime = ZonedDateTime.parse(runContext.render(this.expirationDate)).toOffsetDateTime();

        DataLakeServiceSasSignatureValues signatureValues = new DataLakeServiceSasSignatureValues(offsetDateTime,
            PathSasPermission.parse(this.permissions
                .stream()
                .map(Enum::toString)
                .collect(Collectors.joining()))
        );

        String sas = dataLakeServiceClient.generateSas(signatureValues);

        return Output
            .builder()
            .uri(URI.create(dataLakeServiceClient.getFileUrl() + "?" + sas))
            .build();
    }

    public enum Permission {
        READ("r"),
        ADD("a"),
        CREATE("c"),
        WRITE("w"),
        DELETE("d"),
        LIST("l"),
        MOVE("m"),
        EXECUTE("e"),
        MANAGE_OWNERSHIP("o"),
        MANAGE_ACCESS_CONTROL("p");

        private final String value;

        Permission(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }
    }

    public enum Service {
        BLOB("b"),
        FILE("f"),
        QUEUE("q"),
        TABLE("t");

        private final String value;

        Service(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }
    }

    public enum ResourceType {
        SERVICE("s"),
        CONTAINER("c"),
        OBJECT("o");

        private final String value;

        ResourceType(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The SAS URI."
        )
        private final URI uri;
    }
}
