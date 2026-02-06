package io.kestra.plugin.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

/**
 * This class enables the creation of Azure credentials from different sources.
 * For more information please refer to the <a href="https://learn.microsoft.com/en-us/java/api/overview/azure/identity-readme?view=azure-java-stable">Azure Identity documentation</a>
 *
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractAzureIdentityConnection extends Task implements AzureIdentityConnectionInterface {
    @NotNull
    @Schema(title = "Azure AD tenant ID (GUID)")
    protected Property<String> tenantId;

    @Schema(
        title = "Client ID of the Azure AD application",
        description = "Application (client) ID used for service principal authentication."
    )
    protected Property<String> clientId;
    @Schema(
        title = "Client secret for the Azure AD application",
        description = "Secret value associated with the client ID; store in a Kestra secret."
    )
    protected Property<String> clientSecret;
    @Schema(
        title = "PEM-encoded certificate content for client authentication",
        description = "PEM text for certificate-based auth; alternative to clientSecret."
    )
    protected Property<String> pemCertificate;

    public TokenCredential credentials(RunContext runContext) throws IllegalVariableEvaluationException {
        final String tenantId = runContext.render(this.tenantId).as(String.class).orElse(null);
        final String clientId = runContext.render(this.clientId).as(String.class).orElse(null);

        //Create client/secret credentials
        final String clientSecret = runContext.render(this.clientSecret).as(String.class).orElse(null);
        if(StringUtils.isNotBlank(clientSecret)) {
            runContext.logger().info("Authentication is using Client Secret Credentials");
            return getClientSecretCredential(tenantId, clientId, clientSecret);
        }

        //Create client/certificate credentials
        final String pemCertificate = runContext.render(this.pemCertificate).as(String.class).orElse(null);
        if(StringUtils.isNotBlank(pemCertificate)) {
            runContext.logger().info("Authentication is using Client Certificate Credentials");
            return getClientCertificateCredential(tenantId, clientId, pemCertificate);
        }

        //Create default authentication
        runContext.logger().info("Authentication is using Default Azure Credentials");
        return new DefaultAzureCredentialBuilder().tenantId(tenantId).build();
    }

    private ClientCertificateCredential getClientCertificateCredential(String tenantId, String clientId, String pemCertificate) {
        return new ClientCertificateCredentialBuilder()
                .clientId(clientId)
                .tenantId(tenantId)
                .pemCertificate(new ByteArrayInputStream(StandardCharsets.UTF_8.encode(pemCertificate).array()))
                .build();
    }

    private ClientSecretCredential getClientSecretCredential(String tenantId, String clientId, String clientSecret) {
        return new ClientSecretCredentialBuilder()
                .clientId(clientId)
                .tenantId(tenantId)
                .clientSecret(clientSecret)
                .build();
    }
}
