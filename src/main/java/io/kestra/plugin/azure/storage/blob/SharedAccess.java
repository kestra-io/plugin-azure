package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.fasterxml.jackson.annotation.JsonValue;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageObject;
import io.swagger.v3.oas.annotations.media.Schema;
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
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            code = {
                "endpoint: \"https://yourblob.blob.core.windows.net\"",
                "connectionString: \"DefaultEndpointsProtocol=...==\"",
                "container: \"mydata\"",
                "name: \"myblob\"",
                "expirationDate: \"{{ now() | dateAdd(1, 'DAYS') }}\"",
                "permissions:",
                " - r"
            }
        )
    }
)
@Schema(
    title = "Download a file from an Azure Blob Storage."
)
public class SharedAccess extends AbstractBlobStorageObject implements RunnableTask<SharedAccess.Output> {
    @Schema(
        title = " The time after which the SAS will no longer work."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String expirationDate;

    @Schema(
        title = " The time after which the SAS will no longer work."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Set<Permission> permissions;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BlobClient blobClient = this.blobClient(runContext);

        OffsetDateTime offsetDateTime = ZonedDateTime.parse(runContext.render(this.expirationDate)).toOffsetDateTime();

        BlobServiceSasSignatureValues blobServiceSasSignatureValues = new BlobServiceSasSignatureValues(
            offsetDateTime,
            BlobContainerSasPermission.parse(this.permissions
                .stream()
                .map(Enum::toString)
                .collect(Collectors.joining()))
        );

        String sas = blobClient.generateSas(blobServiceSasSignatureValues);

        return Output
            .builder()
            .uri(URI.create(blobClient.getBlobUrl() + "?" + sas))
            .build();
    }

    public enum Permission {
        READ("r"),
        ADD("a"),
        CREATE("c"),
        WRITE("w"),
        DELETE("d"),
        DELETE_VERSION("x"),
        TAGS("t"),
        LIST("l"),
        MOVE("m"),
        EXECUTE("e"),
        FILTER("f"),
        IMMUTABILITY_POLICY("i");

        private String value;

        Permission(String value) {
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
            title = "The sas url"
        )
        private final URI uri;
    }
}
