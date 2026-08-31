package io.kestra.plugin.azure.horizondb;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

class AbstractHorizonDbUrlTest {
    @Test
    void shouldBuildStandardUrl() throws Exception {
        String url = AbstractHorizonDb.buildJdbcUrl("myserver.postgres.horizondb.azure.com", 5432, "mydb");

        assertThat(url, is("jdbc:postgresql://myserver.postgres.horizondb.azure.com:5432/mydb"));
    }

    @Test
    void shouldPercentEncodeQueryStringInjectionAttempt() throws Exception {
        // A malicious/careless `database` value must not be able to smuggle extra JDBC driver
        // parameters (e.g. disabling TLS, or pointing at an attacker-controlled socketFactory)
        // by breaking out of the path segment with a `?`.
        String url = AbstractHorizonDb.buildJdbcUrl(
            "myserver.postgres.horizondb.azure.com",
            5432,
            "mydb?sslmode=disable&socketFactory=evil.Class"
        );

        assertThat(url, is("jdbc:postgresql://myserver.postgres.horizondb.azure.com:5432/mydb%3Fsslmode=disable&socketFactory=evil.Class"));
        assertThat("the injected `?` must not start a real query string", url, not(containsString(":5432/mydb?")));
    }

    @Test
    void shouldPercentEncodeSpacesInDatabaseName() throws Exception {
        String url = AbstractHorizonDb.buildJdbcUrl("host.example.com", 5432, "my db");

        assertThat(url, is("jdbc:postgresql://host.example.com:5432/my%20db"));
    }

    @Test
    void shouldRejectInvalidHost() {
        org.junit.jupiter.api.Assertions.assertThrows(
            java.net.URISyntaxException.class,
            () -> AbstractHorizonDb.buildJdbcUrl("not a valid host", 5432, "mydb")
        );
    }
}
