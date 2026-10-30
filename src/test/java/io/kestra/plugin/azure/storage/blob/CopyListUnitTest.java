package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.azure.storage.blob.models.CopyListDestination;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class CopyListUnitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldRejectConcurrentAboveMax() {
        var task = CopyList.builder()
            .endpoint(Property.ofValue("http://localhost:10000/devstoreaccount1"))
            .connectionString(Property.ofValue("UseDevelopmentStorage=true"))
            .container(Property.ofValue("src"))
            .to(CopyListDestination.builder().container(Property.ofValue("dst")).build())
            .concurrent(Property.ofValue(65))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
    }
}
