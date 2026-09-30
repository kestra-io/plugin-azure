package io.kestra.plugin.azure.horizondb;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class AbstractHorizonDbConnectionPropertiesTest {
    @Test
    void shouldSetPasswordWhenNotUsingEntraId() {
        Properties props = AbstractHorizonDb.buildConnectionProperties(
            true, "alice", false, "s3cret", null, null, null
        );

        assertThat(props.getProperty("user"), is("alice"));
        assertThat(props.getProperty("password"), is("s3cret"));
        assertThat(props.getProperty("sslmode"), is("require"));
        assertThat(props.getProperty("authenticationPluginClassName"), nullValue());
        assertThat(props.getProperty("azure.tenantId"), nullValue());
    }

    @Test
    void shouldFallBackToDefaultAzureCredentialWhenEntraIdWithNoServicePrincipal() {
        Properties props = AbstractHorizonDb.buildConnectionProperties(
            true, null, true, null, null, null, null
        );

        assertThat(
            props.getProperty("authenticationPluginClassName"),
            is("com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin")
        );
        assertThat(props.getProperty("azure.tenantId"), nullValue());
        assertThat(props.getProperty("azure.clientId"), nullValue());
        assertThat(props.getProperty("azure.clientSecret"), nullValue());
        assertThat(props.getProperty("password"), nullValue());
    }

    @Test
    void shouldSetServicePrincipalPropertiesWhenProvidedWithEntraId() {
        Properties props = AbstractHorizonDb.buildConnectionProperties(
            true, null, true, null, "tenant-1", "client-1", "secret-1"
        );

        assertThat(props.getProperty("authenticationPluginClassName"), notNullValue());
        assertThat(props.getProperty("azure.tenantId"), is("tenant-1"));
        assertThat(props.getProperty("azure.clientId"), is("client-1"));
        assertThat(props.getProperty("azure.clientSecret"), is("secret-1"));
    }

    @Test
    void shouldIgnorePasswordWhenEntraIdIsOnEvenIfSet() {
        Properties props = AbstractHorizonDb.buildConnectionProperties(
            true, "alice", true, "leftover-password", null, null, null
        );

        assertThat(props.getProperty("password"), nullValue());
        assertThat(props.getProperty("user"), is("alice"));
    }

    @Test
    void shouldUsePreferSslModeWhenSslDisabled() {
        Properties props = AbstractHorizonDb.buildConnectionProperties(
            false, null, false, null, null, null, null
        );

        assertThat(props.getProperty("sslmode"), is("prefer"));
    }
}
