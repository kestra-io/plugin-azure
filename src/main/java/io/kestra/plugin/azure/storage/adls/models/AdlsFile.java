package io.kestra.plugin.azure.storage.adls.models;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.PathItem;
import lombok.Builder;
import lombok.Value;
import lombok.With;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Builder
@Value
public class AdlsFile {
    @With
    URI uri;
    String fileSystem;
    String name;
    String fileName;
    Long size;
    String contentType;
    String contentEncoding;
    String contentLanguage;
    String contentMd5;
    Instant creationTime;
    Instant lastModifed;
    String eTag;
    boolean isDirectory;


    public static AdlsFile of(DataLakeFileClient dataLakeFileClient) {
        return AdlsFile.builder()
            .fileSystem(dataLakeFileClient.getFileSystemName())
            .name(dataLakeFileClient.getFilePath())
            .fileName(dataLakeFileClient.getFileName())
            .size(dataLakeFileClient.getProperties().getFileSize())
            .contentType(dataLakeFileClient.getProperties().getContentType())
            .contentEncoding(dataLakeFileClient.getProperties().getContentEncoding())
            .contentLanguage(dataLakeFileClient.getProperties().getContentLanguage())
            .contentMd5(dataLakeFileClient.getProperties().getContentMd5() != null ? new String(dataLakeFileClient.getProperties().getContentMd5(), StandardCharsets.UTF_8) : null)
            .lastModifed(dataLakeFileClient.getProperties().getCreationTime().toInstant())
            .lastModifed(dataLakeFileClient.getProperties().getLastModified().toInstant())
            .eTag(dataLakeFileClient.getProperties().getETag())
            .contentType(dataLakeFileClient.getProperties().getContentType())
            .isDirectory(dataLakeFileClient.getProperties().isDirectory())
            .contentType(dataLakeFileClient.getAccessControl().getGroup())
            .contentType(dataLakeFileClient.getAccessControl().getGroup())
            .contentType(dataLakeFileClient.getAccessControl().getGroup())
            .contentType(dataLakeFileClient.getAccessControl().getGroup())
            .build();
    }

    public static AdlsFile of(String fileSystemName, PathItem pathItem) {
        return AdlsFile.builder()
            .fileSystem(fileSystemName)
            .name(pathItem.getName())
            .size(pathItem.getContentLength())
            .isDirectory(pathItem.isDirectory())
            .build();
    }
}


/*
We capture the following metadata:

Core Attributes
- Name
- Path
- Content Length
- Content Type
- Content Encoding
- Content Language
- Content MD5
- Last Modified
- ETag

Access Control Attributes
- Owner
- Group
- Permissions
- ACLs

Storage Metadata
- Creation Time
- Lease Status
- Lease State
- Lease Duration
- File System Name
- Resource Type

Additional Metadata
- Is Directory
- Archive Status
- Tier
 */