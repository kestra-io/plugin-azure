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
class StartTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldStartInstanceAndReturnId() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement("SELECT df.start(?, ?) AS instance_id")).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("instance_id")).thenReturn("instance-123");

        Start task = Start.builder()
            .id(StartTest.class.getSimpleName())
            .type(Start.class.getName())
            .functionBody(Property.ofValue("'STEP1' ~> 'STEP2'"))
            .label(Property.ofValue("nightly-etl"))
            .build();

        RunContext runContext = runContextFactory.of();
        Start.Output output = task.run(runContext, connection);

        assertThat(output.getInstanceId(), is("instance-123"));
        verify(statement).setObject(1, "'STEP1' ~> 'STEP2'");
        verify(statement).setObject(2, "nightly-etl");
    }
}
