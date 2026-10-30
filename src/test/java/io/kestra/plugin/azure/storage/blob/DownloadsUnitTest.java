package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.azure.shared.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import io.kestra.plugin.azure.storage.blob.services.BlobService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class DownloadsUnitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldDefaultActionToNone() throws Exception {
        var task = Downloads.builder()
            .container(Property.ofValue("my-container"))
            .build();

        var runContext = runContextFactory.of();
        assertThat(task.getAction(), notNullValue());
        assertThat(runContext.render(task.getAction()).as(ActionInterface.Action.class).orElseThrow(), is(ActionInterface.Action.NONE));
    }

    @Test
    void shouldFallbackToNoneWhenActionIsNull() throws Exception {
        var task = Downloads.builder()
            .container(Property.ofValue("my-container"))
            .action(null)
            .build();

        var runContext = runContextFactory.of();
        var rAction = runContext.render(task.getAction())
            .as(ActionInterface.Action.class)
            .orElse(ActionInterface.Action.NONE);

        assertThat(rAction, is(ActionInterface.Action.NONE));
    }

    @Test
    void shouldPreserveSubdirectoriesWithTrailingSlashPrefix() {
        var dest = BlobService.destinationName("archive/", "inbound/sub/report.csv", "inbound/");
        assertThat(dest, is("archive/sub/report.csv"));
    }

    @Test
    void shouldPreserveSubdirectoriesWithPrefixWithoutTrailingSlash() {
        var dest = BlobService.destinationName("archive", "inbound/sub/report.csv", "inbound");
        assertThat(dest, is("archive/sub/report.csv"));
    }

    @Test
    void shouldPreserveDeepSubdirectories() {
        var dest = BlobService.destinationName("archive", "incoming/nested/deep/report.csv", "incoming");
        assertThat(dest, is("archive/nested/deep/report.csv"));
    }

    @Test
    void shouldPreserveFilenameForFlatFileUnderPrefix() {
        var dest = BlobService.destinationName("archive/", "incoming/report.csv", "incoming/");
        assertThat(dest, is("archive/report.csv"));
    }

    @Test
    void shouldPreserveSubdirectoriesWhenPrefixDoesNotEndOnFolderBoundary() {
        var dest1 = BlobService.destinationName("archive/", "data/2026-01/report.csv", "data/2026");
        var dest2 = BlobService.destinationName("archive/", "data/2026-02/report.csv", "data/2026");

        assertThat(dest1, is("archive/2026-01/report.csv"));
        assertThat(dest2, is("archive/2026-02/report.csv"));
        assertThat(dest1, not(is(dest2)));
    }

    @Test
    void shouldFallbackToFilenameWhenPrefixIsNull() {
        var dest = BlobService.destinationName("archive", "incoming/report.csv", null);
        assertThat(dest, is("archive/report.csv"));
    }

    @Test
    void shouldFallbackToFilenameWhenPrefixDoesNotMatch() {
        var dest = BlobService.destinationName("archive/", "other-folder/report.csv", "incoming");
        assertThat(dest, is("archive/report.csv"));
    }

    @Test
    void shouldHandleLeadingSlashesInPrefixAndNames() {
        var dest = BlobService.destinationName("/tasks/archive", "/tasks/from/sub/file.yml", "/tasks/from");
        assertThat(dest, is("/tasks/archive/sub/file.yml"));
    }

    @Test
    void shouldHandleEmptyMoveToName() {
        var dest = BlobService.destinationName("", "incoming/2026/report.csv", "incoming/");
        assertThat(dest, is("2026/report.csv"));
    }

    @Test
    void shouldIdentifyDirectoryMarkers() {
        assertThat(BlobService.isDirectoryMarker("folder/"), is(true));
        assertThat(BlobService.isDirectoryMarker("folder/sub/"), is(true));
        assertThat(BlobService.isDirectoryMarker(Blob.builder().name("folder/").build()), is(true));
        assertThat(BlobService.isDirectoryMarker("folder/file.csv"), is(false));
        assertThat(BlobService.isDirectoryMarker(Blob.builder().name("folder/file.csv").build()), is(false));
        assertThat(BlobService.isDirectoryMarker((String) null), is(false));
        assertThat(BlobService.isDirectoryMarker((Blob) null), is(false));
    }

    @Test
    void shouldThrowWhenMoveTargetsFindsCollision() {
        var runContext = runContextFactory.of();
        var blobs = List.of(
            Blob.builder().container("raw").name("folderA/report.csv").build(),
            Blob.builder().container("raw").name("folderB/report.csv").build()
        );

        var moveTo = Copy.CopyObject.builder()
            .name(Property.ofValue("archive/"))
            .build();

        var ex = assertThrows(
            IllegalStateException.class,
            () -> BlobService.moveTargets(
                blobs,
                ActionInterface.Action.MOVE,
                moveTo,
                runContext,
                null
            )
        );

        assertThat(ex.getMessage(), containsString("multiple source blobs would overwrite the same destination name"));
        assertThat(ex.getMessage(), containsString("Set a `prefix` that covers both folders"));
    }

    @Test
    void shouldPreserveCustomMoveToContainer() throws Exception {
        var runContext = runContextFactory.of();
        var blobs = List.of(
            Blob.builder().container("raw-container").name("folderA/report.csv").build()
        );

        var moveTo = Copy.CopyObject.builder()
            .container(Property.ofValue("custom-archive-container"))
            .name(Property.ofValue("archive/"))
            .build();

        var targets = BlobService.moveTargets(
            blobs,
            ActionInterface.Action.MOVE,
            moveTo,
            runContext,
            Property.ofValue("folderA/")
        );

        assertThat(targets.size(), is(1));
        assertThat(targets.getFirst().getRight(), is("archive/report.csv"));
        assertThat(runContext.render(moveTo.getContainer()).as(String.class).orElseThrow(), is("custom-archive-container"));
    }
}
