package io.kestra.plugin.azure.storage.table.models;

import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableTransactionActionType;
import lombok.Builder;
import lombok.Getter;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@Builder
public class Entity {
    String partitionKey;
    String rowKey;
    OffsetDateTime timestamp;
    String etag;
    Map<String, Object> properties;
    TableTransactionActionType type;

    public static Entity to(TableEntity tableEntity) {
        return Entity.builder()
            .partitionKey(tableEntity.getPartitionKey())
            .rowKey(tableEntity.getRowKey())
            .timestamp(tableEntity.getTimestamp())
            .etag(tableEntity.getETag())
            .properties(tableEntity
                .getProperties()
                .entrySet()
                .stream()
                .filter(e -> !e.getKey().startsWith("odata") &&
                    !e.getKey().startsWith("Timestamp") &&
                    !e.getKey().equals("PartitionKey") &&
                    !e.getKey().equals("RowKey") &&
                    !e.getKey().contains("@odata")
                )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            )
            .build();
    }

    public TableEntity to() {
        TableEntity tableEntity = new TableEntity(this.getPartitionKey(), this.getRowKey());

        tableEntity.setProperties(this.getProperties());

        return tableEntity;
    }

}
