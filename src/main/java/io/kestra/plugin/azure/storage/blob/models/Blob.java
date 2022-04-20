package io.kestra.plugin.azure.storage.blob.models;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import lombok.Builder;
import lombok.Value;
import lombok.With;

import java.net.URI;

@Builder
@Value
public class Blob {
    @With
    URI uri;
    String container;
    String name;
    Long size;

    public static Blob of(BlobClient blobClient) {
        return Blob.of(blobClient, blobClient.getProperties());
    }

    public static Blob of(BlobClient blobClient, BlobProperties blobProperties) {
        return Blob.builder()
            .container(blobClient.getContainerName())
            .name(blobClient.getBlobName())
            .size(blobProperties.getBlobSize())
            .build();
    }

    public static Blob of(String container, BlobItem blobItem) {
        return Blob.builder()
            .container(container)
            .name(blobItem.getName())
            .size(blobItem.getProperties().getContentLength())
            .build();
    }
}
