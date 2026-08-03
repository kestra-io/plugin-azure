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
class GetStatusTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldReturnStatusAndResult() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement("SELECT df.status(?) AS status, df.result(?) AS result")).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("status")).thenReturn("Completed");
        when(resultSet.getString("result")).thenReturn("{\"rows\": 42}");

        GetStatus task = GetStatus.builder()
            .id(GetStatusTest.class.getSimpleName())
            .type(GetStatus.class.getName())
            .instanceId(Property.ofValue("instance-123"))
            .build();

        RunContext runContext = runContextFactory.of();
        GetStatus.Output output = task.run(runContext, connection);

        assertThat(output.getInstanceId(), is("instance-123"));
        assertThat(output.getStatus(), is("Completed"));
        assertThat(output.getResult(), is("{\"rows\": 42}"));
        verify(statement).setObject(1, "instance-123");
        verify(statement).setObject(2, "instance-123");
    }
}
