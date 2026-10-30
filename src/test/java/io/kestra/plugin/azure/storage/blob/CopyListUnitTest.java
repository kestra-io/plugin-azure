package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.storage.blob.models.CopyListDestination;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class CopyListUnitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ModelValidator modelValidator;

    @Test
    void shouldPassBeanValidationWithoutConcurrent() {
        var task = base(CopyListDestination.builder().container(Property.ofValue("dst")).build()).build();

        assertThat(modelValidator.isValid(task).isPresent(), is(false));
    }

    @Test
    void shouldPassBeanValidationWithValidConcurrent() {
        var task = base(CopyListDestination.builder().container(Property.ofValue("dst")).build())
            .concurrent(Property.ofValue(8))
            .build();

        assertThat(modelValidator.isValid(task).isPresent(), is(false));
    }

    @Test
    void shouldRejectOutOfRangeConcurrentInBeanValidation() {
        var to = CopyListDestination.builder().container(Property.ofValue("dst")).build();
        var low = base(to).concurrent(Property.ofValue(1)).build();
        var high = base(to).concurrent(Property.ofValue(100)).build();

        assertThat(modelValidator.isValid(low).isPresent(), is(true));
        assertThat(modelValidator.isValid(high).isPresent(), is(true));
    }

    @Test
    void shouldRejectConcurrentAboveMax() {
        var task = CopyList.builder()
            .id(IdUtils.create())
            .type(CopyList.class.getName())
            .endpoint(Property.ofValue("http://localhost:10000/devstoreaccount1"))
            .connectionString(Property.ofValue("UseDevelopmentStorage=true"))
            .container(Property.ofValue("src"))
            .to(CopyListDestination.builder().container(Property.ofValue("dst")).build())
            .concurrent(Property.ofValue(65))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
    }

    private CopyList.CopyListBuilder<?, ?> base(CopyListDestination to) {
        return CopyList.builder()
            .id(IdUtils.create())
            .type(CopyList.class.getName())
            .endpoint(Property.ofValue("http://localhost:10000/devstoreaccount1"))
            .connectionString(Property.ofValue("UseDevelopmentStorage=true"))
            .container(Property.ofValue("src"))
            .prefix(Property.ofValue("a/"))
            .to(to);
    }

    @Test
    void shouldRejectSameContainerWithoutDestinationPrefix() {
        var task = base(CopyListDestination.builder().container(Property.ofValue("src")).build())
            .delete(Property.ofValue(true))
            .build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("same blobs"));
    }

    @Test
    void shouldRejectSameContainerWithSamePrefix() {
        var task = base(CopyListDestination.builder()
            .container(Property.ofValue("src"))
            .prefix(Property.ofValue("a/"))
            .build()
        ).delete(Property.ofValue(true)).build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("same blobs"));
    }

    @Test
    void shouldRejectSameContainerWithEmptyDestinationPrefix() {
        var task = base(CopyListDestination.builder()
            .container(Property.ofValue("src"))
            .prefix(Property.ofValue(""))
            .build()
        ).prefix(null).delete(Property.ofValue(true)).build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("same blobs"));
    }

    @Test
    void shouldRejectDestinationNestedInSourcePrefix() {
        var task = base(CopyListDestination.builder()
            .container(Property.ofValue("src"))
            .prefix(Property.ofValue("a/processed/"))
            .build()
        ).delete(Property.ofValue(true)).build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("inside the listed source range"));
    }

    @Test
    void shouldRejectConnectionStringWithoutAccountKey() {
        var task = base(CopyListDestination.builder().container(Property.ofValue("dst")).build())
            .connectionString(Property.ofValue("BlobEndpoint=http://localhost:10000/devstoreaccount1;SharedAccessSignature=sv=2022-11-02&sig=abc"))
            .build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("AccountKey"));
    }

    @Test
    void shouldRejectSharedKeyWithoutAccountName() {
        var task = base(CopyListDestination.builder().container(Property.ofValue("dst")).build())
            .connectionString(null)
            .sharedKeyAccountAccessKey(Property.ofValue("a2V5"))
            .build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("sharedKeyAccountName"));
    }

    @Test
    void shouldRejectSasTokenAuth() {
        var task = CopyList.builder()
            .id(IdUtils.create())
            .type(CopyList.class.getName())
            .endpoint(Property.ofValue("http://localhost:10000/devstoreaccount1"))
            .sasToken(Property.ofValue("sv=2022-11-02&sig=abc"))
            .container(Property.ofValue("src"))
            .to(CopyListDestination.builder().container(Property.ofValue("dst")).build())
            .build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("sasToken"));
    }

    @Test
    void shouldRequireDestinationContainer() {
        var task = base(CopyListDestination.builder().build()).build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("to.container"));
    }

    @Test
    void shouldComputeDestinationName() {
        assertThat(CopyList.destinationName("a/b/c.yml", "a/", "z/"), is("z/b/c.yml"));
        assertThat(CopyList.destinationName("a/b/c.yml", null, "z/"), is("z/a/b/c.yml"));
        assertThat(CopyList.destinationName("a/b/c.yml", "x/", "z/"), is("z/a/b/c.yml"));
        assertThat(CopyList.destinationName("a/b/c.yml", "a/", null), is("a/b/c.yml"));
        assertThat(CopyList.destinationName("a/b/c.yml", null, null), is("a/b/c.yml"));
    }
}
