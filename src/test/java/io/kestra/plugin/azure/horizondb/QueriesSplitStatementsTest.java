package io.kestra.plugin.azure.horizondb;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class QueriesSplitStatementsTest {
    @Test
    void shouldSplitOnSemicolons() {
        List<String> statements = Queries.splitStatements(
            "DELETE FROM staging WHERE loaded_at < now() - INTERVAL '1 day';\n" +
                "INSERT INTO target SELECT * FROM staging;"
        );

        assertThat(statements, hasSize(2));
        assertThat(statements.get(0), is("DELETE FROM staging WHERE loaded_at < now() - INTERVAL '1 day'"));
        assertThat(statements.get(1), is("INSERT INTO target SELECT * FROM staging"));
    }

    @Test
    void shouldIgnoreBlankStatements() {
        List<String> statements = Queries.splitStatements("SELECT 1;;   ;\nSELECT 2;");

        assertThat(statements, hasSize(2));
        assertThat(statements.get(0), is("SELECT 1"));
        assertThat(statements.get(1), is("SELECT 2"));
    }

    @Test
    void shouldHandleSingleStatementWithoutTrailingSemicolon() {
        List<String> statements = Queries.splitStatements("SELECT 1");

        assertThat(statements, hasSize(1));
        assertThat(statements.get(0), is("SELECT 1"));
    }
}
