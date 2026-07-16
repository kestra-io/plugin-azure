package io.kestra.plugin.azure.horizondb.durable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

@KestraTest
class ListInstancesTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldFilterByStatusAndFetchRows() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(connection.prepareStatement("SELECT * FROM df.list_instances() WHERE status = ?")).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(2);
        when(metaData.getColumnLabel(1)).thenReturn("instance_id");
        when(metaData.getColumnLabel(2)).thenReturn("status");
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getObject(1)).thenReturn("instance-1", "instance-2");
        when(resultSet.getObject(2)).thenReturn("Completed", "Completed");

        ListInstances task = ListInstances.builder()
            .id(ListInstancesTest.class.getSimpleName())
            .type(ListInstances.class.getName())
            .statusFilter(Property.ofValue("Completed"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext runContext = runContextFactory.of();
        ListInstances.Output output = task.run(runContext, connection);

        assertThat(output.getInstances(), hasSize(2));
        assertThat(output.getSize(), is(2L));
        verify(statement).setObject(1, "Completed");
    }

    @Test
    void shouldListAllInstancesWhenNoFilter() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(connection.prepareStatement("SELECT * FROM df.list_instances()")).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("instance_id");
        when(resultSet.next()).thenReturn(false);

        ListInstances task = ListInstances.builder()
            .id(ListInstancesTest.class.getSimpleName())
            .type(ListInstances.class.getName())
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext runContext = runContextFactory.of();
        ListInstances.Output output = task.run(runContext, connection);

        assertThat(output.getInstances(), hasSize(0));
        verify(statement, never()).setObject(anyInt(), any());
    }
}
