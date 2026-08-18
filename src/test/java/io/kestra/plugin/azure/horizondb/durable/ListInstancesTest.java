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

        when(connection.prepareStatement("SELECT * FROM df.list_instances(?, ?)")).thenReturn(statement);
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
        // status filter is passed as df.list_instances()'s own first argument
        verify(statement).setObject(1, "Completed");
        // limit wasn't set explicitly, but it now defaults to 1000 rather than being left
        // null/unbounded
        verify(statement).setObject(2, 1000);
    }

    @Test
    void shouldPassLimitAsSecondArgumentWhenSet() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("instance_id");
        when(resultSet.next()).thenReturn(false);

        ListInstances task = ListInstances.builder()
            .id(ListInstancesTest.class.getSimpleName())
            .type(ListInstances.class.getName())
            .limit(Property.ofValue(10))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext runContext = runContextFactory.of();
        task.run(runContext, connection);

        verify(statement).setObject(2, 10);
    }

    @Test
    void shouldListAllInstancesWhenNoFilterOrLimit() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(connection.prepareStatement("SELECT * FROM df.list_instances(?, ?)")).thenReturn(statement);
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
        verify(statement).setNull(eq(1), anyInt());
        verify(statement).setObject(2, 1000);
    }

    @Test
    void shouldStoreInstancesToInternalStorage() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(connection.prepareStatement("SELECT * FROM df.list_instances(?, ?)")).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("instance_id");
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getObject(1)).thenReturn("i-1", "i-2");

        ListInstances task = ListInstances.builder()
            .id(ListInstancesTest.class.getSimpleName())
            .type(ListInstances.class.getName())
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();

        RunContext runContext = runContextFactory.of();
        ListInstances.Output output = task.run(runContext, connection);

        assertThat(output.getUri(), notNullValue());
        assertThat(output.getSize(), is(2L));
        assertThat(output.getInstances(), nullValue());
        verify(statement).setFetchSize(10000);
    }
}
