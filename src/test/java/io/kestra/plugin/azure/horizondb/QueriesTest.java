package io.kestra.plugin.azure.horizondb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

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
class QueriesTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldRunEachStatementInOrderAndAggregateOutputs() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);

        // statement 1: DELETE, no result set, 4 rows affected
        // statement 2: SELECT, returns a result set with one row
        when(statement.execute(anyString())).thenReturn(false, true);
        when(statement.getUpdateCount()).thenReturn(4);
        when(statement.getResultSet()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("id");
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getObject(1)).thenReturn(42);

        Queries task = Queries.builder()
            .id(QueriesTest.class.getSimpleName())
            .type(Queries.class.getName())
            .sql(Property.ofValue("DELETE FROM staging WHERE loaded_at < now();\nINSERT INTO target SELECT * FROM staging RETURNING id;"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        RunContext runContext = runContextFactory.of();
        Queries.Output output = task.run(runContext, connection);

        assertThat(output.getOutputs(), hasSize(2));
        assertThat(output.getOutputs().get(0).getUpdateCount(), is(4L));
        assertThat(output.getOutputs().get(0).getRows(), nullValue());
        assertThat(output.getOutputs().get(1).getRows(), hasSize(1));
        assertThat(output.getOutputs().get(1).getUpdateCount(), nullValue());

        verify(statement, times(2)).execute(anyString());
        verify(statement).execute("DELETE FROM staging WHERE loaded_at < now()");
        verify(statement).execute("INSERT INTO target SELECT * FROM staging RETURNING id");
        // both statements run over the same Statement/Connection, not one each
        verify(connection, times(1)).createStatement(anyInt(), anyInt());

        // fetch.size is emitted once for the SELECT statement (1 row); the DELETE has no result
        // set / no size, so it must not emit a spurious 0-row metric
        long fetchSizeMetricCount = runContext.metrics().stream()
            .filter(m -> m.getName().equals("fetch.size"))
            .count();
        assertThat(fetchSizeMetricCount, is(1L));
    }

    @Test
    void shouldStoreResultSetStatementsToInternalStorage() throws Exception {
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
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getObject(1)).thenReturn(1);

        Queries task = Queries.builder()
            .id(QueriesTest.class.getSimpleName())
            .type(Queries.class.getName())
            .sql(Property.ofValue("SELECT id FROM t;"))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();

        RunContext runContext = runContextFactory.of();
        Queries.Output output = task.run(runContext, connection);

        assertThat(output.getOutputs(), hasSize(1));
        assertThat(output.getOutputs().get(0).getUri(), notNullValue());
        verify(statement).setFetchSize(10000);
    }

    @Test
    void shouldFailFastWhenSqlIsMissing() {
        Queries task = Queries.builder()
            .id(QueriesTest.class.getSimpleName())
            .type(Queries.class.getName())
            .build();

        Connection connection = mock(Connection.class);
        RunContext runContext = runContextFactory.of();

        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContext, connection)
        );

        verifyNoInteractions(connection);
    }
}
