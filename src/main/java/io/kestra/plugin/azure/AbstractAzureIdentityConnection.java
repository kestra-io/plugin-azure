package io.kestra.plugin.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
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
    @Builder.Default
    protected Property<String> tenantId = Property.of("");

    @Builder.Default
    protected Property<String> clientId = Property.of("");
    @Builder.Default
    protected Property<String> clientSecret = Property.of("");
    @Builder.Default
    protected Property<String> pemCertificate = Property.of("");

    public TokenCredential credentials(RunContext runContext) throws IllegalVariableEvaluationException {
        final String tenantId = this.tenantId.as(runContext, String.class);
        final String clientId = this.clientId.as(runContext, String.class);

        //Create client/secret credentials
        final String clientSecret = this.clientSecret.as(runContext, String.class);
        if(StringUtils.isNotBlank(clientSecret)) {
            runContext.logger().info("Authentication is using Client Secret Credentials");
            return getClientSecretCredential(tenantId, clientId, clientSecret);
        }

        //Create client/certificate credentials
        final String pemCertificate = this.pemCertificate.as(runContext, String.class);
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
