package io.kestra.plugin.azure.eventhubs.service.consumer;

public record EventHubNamePartition(String eventHubName,
                                    String partitionId) {
}

