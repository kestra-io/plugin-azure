package io.kestra.plugin.azure.horizondb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@KestraTest
class QueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldFetchOneRow() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.execute("SELECT id, name FROM t")).thenReturn(true);
        when(statement.getResultSet()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(2);
        when(metaData.getColumnLabel(1)).thenReturn("id");
        when(metaData.getColumnLabel(2)).thenReturn("name");
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getObject(1)).thenReturn(1);
        when(resultSet.getObject(2)).thenReturn("foo");

        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql(Property.ofValue("SELECT id, name FROM t"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        RunContext runContext = runContextFactory.of();
        Query.Output output = task.run(runContext, connection);

        assertThat(output.getRow(), is(Map.of("id", 1, "name", "foo")));
        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows(), nullValue());
        assertThat(output.getUri(), nullValue());
    }

    @Test
    void shouldFetchAllRows() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(true);
        when(statement.getResultSet()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("id");
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getObject(1)).thenReturn(1, 2);

        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql(Property.ofValue("SELECT id FROM t"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext runContext = runContextFactory.of();
        Query.Output output = task.run(runContext, connection);

        assertThat(output.getRows(), hasSize(2));
        assertThat(output.getSize(), is(2L));

        long fetchSizeMetricValue = runContext.metrics().stream()
            .filter(m -> m.getName().equals("fetch.size"))
            .mapToLong(m -> ((Number) m.getValue()).longValue())
            .sum();
        assertThat(fetchSizeMetricValue, is(2L));
    }

    @Test
    void shouldReturnUpdateCountForNonResultSetStatement() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);

        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(false);
        when(statement.getUpdateCount()).thenReturn(3);

        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql(Property.ofValue("DELETE FROM t WHERE id = 1"))
            .fetchType(Property.ofValue(FetchType.NONE))
            .build();

        RunContext runContext = runContextFactory.of();
        Query.Output output = task.run(runContext, connection);

        assertThat(output.getUpdateCount(), is(3L));
        assertThat(output.getRow(), nullValue());
        assertThat(output.getRows(), nullValue());
    }

    @Test
    void shouldStoreRowsToInternalStorage() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(true);
        when(statement.getResultSet()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("id");
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getObject(1)).thenReturn(1, 2);

        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql(Property.ofValue("SELECT id FROM t"))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();

        RunContext runContext = runContextFactory.of();
        Query.Output output = task.run(runContext, connection);

        assertThat(output.getUri(), notNullValue());
        assertThat(output.getSize(), is(2L));
        assertThat(output.getRow(), nullValue());
        assertThat(output.getRows(), nullValue());
        verify(statement).setFetchSize(10000);

        // verify the stored ION content actually contains both rows
        java.util.List<Object> rows = new java.util.ArrayList<>();
        // ION is written as binary, so read the stream directly rather than through a text Reader
        try (java.io.InputStream storedContent = runContext.storage().getFile(output.getUri())) {
            io.kestra.core.serializers.FileSerde.readAll(storedContent)
                .doOnNext(rows::add)
                .blockLast();
        }
        assertThat(rows, hasSize(2));
    }

    @Test
    void shouldFailFastWhenSqlIsMissing() {
        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .build();

        Connection connection = mock(Connection.class);
        RunContext runContext = runContextFactory.of();

        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContext, connection)
        );

        verifyNoInteractions(connection);
    }

    @Test
    void shouldPropagateSqlExceptionFromTheDriver() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);

        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.execute(anyString())).thenThrow(new java.sql.SQLException("syntax error at or near \"SELCT\""));

        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql(Property.ofValue("SELCT 1"))
            .build();

        RunContext runContext = runContextFactory.of();

        java.sql.SQLException thrown = org.junit.jupiter.api.Assertions.assertThrows(
            java.sql.SQLException.class,
            () -> task.run(runContext, connection)
        );
        assertThat(thrown.getMessage(), containsString("syntax error"));
        // the statement must still be closed even though execute() threw
        verify(statement).close();
    }

    @Test
    void shouldCancelTheRunningStatementOnKill() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(true);
        when(statement.getResultSet()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("id");
        when(resultSet.next()).thenReturn(false);
        // simulate that the statement is still open at the moment kill() is invoked
        when(statement.isClosed()).thenReturn(false);

        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql(Property.ofValue("SELECT id FROM t"))
            .build();

        RunContext runContext = runContextFactory.of();
        task.run(runContext, connection);

        // run() has already returned (and closed the statement via try-with-resources), but
        // kill() must still be safe to call and must attempt cancel()/close() on whatever
        // statement was last tracked
        task.kill();

        verify(statement, atLeastOnce()).cancel();
    }

    @Test
    void shouldNotThrowAndStillCloseTheConnectionWhenCancelFails() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(true);
        when(statement.getResultSet()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("id");
        when(resultSet.next()).thenReturn(false);
        when(statement.isClosed()).thenReturn(false);
        when(connection.isClosed()).thenReturn(false);
        // cancelling the in-flight statement itself fails
        doThrow(new java.sql.SQLException("driver refused to cancel")).when(statement).cancel();

        Query task = Query.builder()
            .id(QueryTest.class.getSimpleName())
            .type(Query.class.getName())
            .sql(Property.ofValue("SELECT id FROM t"))
            .build();

        RunContext runContext = runContextFactory.of();
        task.run(runContext, connection);

        // simulate kill() being invoked while a connection is still tracked, i.e. mid-run(RunContext)
        java.lang.reflect.Field runningConnectionField = io.kestra.plugin.azure.horizondb.AbstractHorizonDb.class
            .getDeclaredField("runningConnection");
        runningConnectionField.setAccessible(true);
        runningConnectionField.set(task, connection);

        // kill() must never throw, even though statement.cancel() does
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(task::kill);

        // ...and must still close the connection despite the statement cancel failure, instead
        // of aborting early and leaking it
        verify(connection).close();
    }
}
