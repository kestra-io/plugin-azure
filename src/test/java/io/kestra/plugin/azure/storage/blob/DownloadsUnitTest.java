package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.azure.shared.storage.blob.abstracts.ListInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
    void shouldDefaultFilterToFiles() throws Exception {
        var task = Downloads.builder()
            .container(Property.ofValue("my-container"))
            .build();

        var runContext = runContextFactory.of();
        assertThat(task.getFilter(), notNullValue());
        assertThat(runContext.render(task.getFilter()).as(ListInterface.Filter.class).orElseThrow(), is(ListInterface.Filter.FILES));
    }

    @Test
    void shouldAllowCustomFilter() throws Exception {
        var task = Downloads.builder()
            .container(Property.ofValue("my-container"))
            .filter(Property.ofValue(ListInterface.Filter.DIRECTORY))
            .build();

        var runContext = runContextFactory.of();
        assertThat(task.getFilter(), notNullValue());
        assertThat(runContext.render(task.getFilter()).as(ListInterface.Filter.class).orElseThrow(), is(ListInterface.Filter.DIRECTORY));
    }
}
