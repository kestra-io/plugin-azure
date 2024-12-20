package io.kestra.plugin.azure.auth;

import com.azure.core.credential.*;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.EncryptedString;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.AbstractAzureIdentityConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: azure_get_token
                namespace: company.team

                tasks:
                  - id: get_access_token
                    type: io.kestra.plugin.azure.oauth.OauthAccessToken
                    tenantId: "{{ secret('SERVICE_PRINCIPAL_TENANT_ID') }}"
                    clientId: "{{ secret('SERVICE_PRINCIPAL_CLIENT_ID') }}"
                    clientSecret: "{{ secret('SERVICE_PRINCIPAL_CLIENT_SECRET') }}"
                """
        )
    }
)
@Schema(
    title = "Fetch an OAuth access token."
)
public class OauthAccessToken extends AbstractAzureIdentityConnection implements RunnableTask<OauthAccessToken.Output> {
    @Schema(title = "The Azure scopes to be used")
    @Builder.Default
    Property<List<String>> scopes = Property.of(Collections.singletonList("https://management.azure.com/.default"));

    @Override
    public Output run(RunContext runContext) throws Exception {
        TokenCredential credential = this.credentials(runContext);

        TokenRequestContext requestContext = new TokenRequestContext();
        requestContext.setScopes(runContext.render(scopes).asList(String.class));

        runContext.logger().info("Retrieve access token.");
        AccessToken accessToken = credential.getTokenSync(requestContext);

        runContext.logger().info("Successfully retrieved access token.");

        var output = AccessTokenOutput.builder()
            .expirationTime(accessToken.getExpiresAt())
            .scopes(requestContext.getScopes())
            .tokenValue(EncryptedString.from(accessToken.getToken(), runContext));

        return Output
            .builder()
            .accessToken(output.build())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @NotNull
        @Schema(title = "An OAuth access token for the current user.")
        private final AccessTokenOutput accessToken;
    }

    @Builder
    @Getter
    public static class AccessTokenOutput {
        List<String> scopes;

        @Schema(
            title = "OAuth access token value",
            description = "Will be automatically encrypted and decrypted in the outputs if encryption is configured"
        )
        EncryptedString tokenValue;

        OffsetDateTime expirationTime;
    }
}
