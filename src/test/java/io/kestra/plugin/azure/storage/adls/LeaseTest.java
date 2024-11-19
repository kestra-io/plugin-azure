package io.kestra.plugin.azure.storage.adls;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.storage.blob.List;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class LeaseTest extends AbstractTest {
    @Test
    void run() throws Exception {
        String prefix = IdUtils.create();

        Upload.Output upload = upload("adls/azure/" + prefix);

        // Acquire Lease
        Lease acquireTask = Lease.builder()
            .id(LeaseTest.class.getSimpleName())
            .type(Lease.class.getName())
            .endpoint(this.adlsEndpoint)
            .connectionString(this.connectionString)
            .fileSystem(this.fileSystem)
            .fileName(upload.getFile().getName())
            .action(Property.of(Lease.LeaseAction.ACQUIRE))
            .leaseDuration(Property.of(30))
            .build();

        Lease.Output acquire = acquireTask.run(runContext(acquireTask));

        assertThat(acquire.getId(), is(notNullValue()));

        // Acquire Lease
        Lease releaseTask = Lease.builder()
            .id(LeaseTest.class.getSimpleName())
            .type(Lease.class.getName())
            .endpoint(this.adlsEndpoint)
            .connectionString(this.connectionString)
            .fileSystem(this.fileSystem)
            .fileName(upload.getFile().getName())
            .action(Property.of(Lease.LeaseAction.RELEASE))
            .leaseId(Property.of(acquire.getId()))
            .build();

        Lease.Output release = releaseTask.run(runContext(releaseTask));

        assertThat(release.getId(), is(notNullValue()));
    }
}
