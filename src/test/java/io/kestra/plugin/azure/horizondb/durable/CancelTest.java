package io.kestra.plugin.azure.horizondb.durable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@KestraTest
class CancelTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldCancelInstance() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement("SELECT df.cancel(?) AS cancelled")).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean("cancelled")).thenReturn(true);

        Cancel task = Cancel.builder()
            .id(CancelTest.class.getSimpleName())
            .type(Cancel.class.getName())
            .instanceId(Property.ofValue("instance-123"))
            .build();

        RunContext runContext = runContextFactory.of();
        Cancel.Output output = task.run(runContext, connection);

        assertThat(output.getInstanceId(), is("instance-123"));
        assertThat(output.getCancelled(), is(true));
        verify(statement).setObject(1, "instance-123");
    }

    @Test
    void shouldReturnFalseWhenNoRowReturned() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        Cancel task = Cancel.builder()
            .id(CancelTest.class.getSimpleName())
            .type(Cancel.class.getName())
            .instanceId(Property.ofValue("unknown"))
            .build();

        RunContext runContext = runContextFactory.of();
        Cancel.Output output = task.run(runContext, connection);

        assertThat(output.getCancelled(), is(false));
    }
}
