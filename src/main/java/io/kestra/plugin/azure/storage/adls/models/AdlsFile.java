package io.kestra.plugin.azure.storage.adls.models;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.*;
import lombok.Builder;
import lombok.Value;
import lombok.With;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

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

    LeaseStateType leaseState;
    LeaseDurationType leaseDuration;
    LeaseStatusType leaseStatus;

    boolean isDirectory;
    ArchiveStatus archiveStatus;
    AccessTier archiveTier;

    String owner;
    String group;
    String permissions;
    List<String> accessControlList;



    public static AdlsFile of(DataLakeFileClient dataLakeFileClient) {
        return AdlsFile.builder()
            .fileSystem(dataLakeFileClient.getFileSystemName())
            .name(dataLakeFileClient.getFilePath())
            .fileName(dataLakeFileClient.getFileName())
            .size(dataLakeFileClient.getProperties().getFileSize())
            .contentType(dataLakeFileClient.getProperties().getContentType())
            .contentEncoding(dataLakeFileClient.getProperties().getContentEncoding())
            .contentLanguage(dataLakeFileClient.getProperties().getContentLanguage())
            .contentMd5(dataLakeFileClient.getProperties().getContentMd5() != null
                ? Base64.getEncoder().encodeToString(dataLakeFileClient.getProperties().getContentMd5())
                : null)
            .lastModifed(dataLakeFileClient.getProperties().getCreationTime().toInstant())
            .lastModifed(dataLakeFileClient.getProperties().getLastModified().toInstant())
            .eTag(dataLakeFileClient.getProperties().getETag())
            .isDirectory(dataLakeFileClient.getProperties().isDirectory())
            .leaseState(dataLakeFileClient.getProperties().getLeaseState())
            .leaseDuration(dataLakeFileClient.getProperties().getLeaseDuration())
            .leaseStatus(dataLakeFileClient.getProperties().getLeaseStatus())
            .archiveStatus(dataLakeFileClient.getProperties().getArchiveStatus())
            .archiveTier(dataLakeFileClient.getProperties().getAccessTier())
            .owner(dataLakeFileClient.getProperties().getOwner())
            .group(dataLakeFileClient.getProperties().getGroup())
            .permissions(dataLakeFileClient.getProperties().getPermissions())
            .accessControlList(dataLakeFileClient.getProperties().getAccessControlList().stream().map(PathAccessControlEntry::toString).toList())
            .build();
    }
}