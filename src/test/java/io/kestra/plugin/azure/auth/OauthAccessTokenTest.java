package io.kestra.plugin.azure.auth;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class OauthAccessTokenTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Disabled("To run this test make sure your are logged in via the command line with 'az login'")
    @Test
    void getAccessTokenFromDefaultCredential() throws Exception {
        OauthAccessToken task = OauthAccessToken.builder()
            .build();

        OauthAccessToken.Output run = task.run(runContextFactory.of(Collections.emptyMap()));

        OauthAccessToken.AccessTokenOutput accessToken = run.getAccessToken();
        assertThat(accessToken, notNullValue());
    }

    @Disabled("To run this test provide your service principal credentials")
    @Test
    void getAccessTokenFromClientSecretCredential() throws Exception {
        final String tenantId = "";
        final String clientId = "";
        final String clientSecret = "";

        OauthAccessToken task = OauthAccessToken.builder()
            .tenantId(Property.of(tenantId))
            .clientId(Property.of(clientId))
            .clientSecret(Property.of(clientSecret))
            .build();

        OauthAccessToken.Output run = task.run(runContextFactory.of(Collections.emptyMap()));

        OauthAccessToken.AccessTokenOutput accessToken = run.getAccessToken();
        assertThat(accessToken, notNullValue());
    }
}