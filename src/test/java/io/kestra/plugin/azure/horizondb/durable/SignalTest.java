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
import static org.mockito.Mockito.*;

@KestraTest
class SignalTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldSignalInstance() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement("SELECT df.signal(?, ?, ?) AS signaled")).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean("signaled")).thenReturn(true);

        Signal task = Signal.builder()
            .id(SignalTest.class.getSimpleName())
            .type(Signal.class.getName())
            .instanceId(Property.ofValue("instance-123"))
            .signalName(Property.ofValue("approval"))
            .payload(Property.ofValue("{\"approved\": true}"))
            .build();

        RunContext runContext = runContextFactory.of();
        Signal.Output output = task.run(runContext, connection);

        assertThat(output.getInstanceId(), is("instance-123"));
        assertThat(output.getSignaled(), is(true));
        verify(statement).setObject(1, "instance-123");
        verify(statement).setObject(2, "approval");
        verify(statement).setObject(3, "{\"approved\": true}");
    }

    @Test
    void shouldBindNullWhenPayloadAbsent() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean("signaled")).thenReturn(true);

        Signal task = Signal.builder()
            .id(SignalTest.class.getSimpleName())
            .type(Signal.class.getName())
            .instanceId(Property.ofValue("instance-123"))
            .signalName(Property.ofValue("approval"))
            .build();

        RunContext runContext = runContextFactory.of();
        task.run(runContext, connection);

        verify(statement).setNull(eq(3), anyInt());
    }
}
