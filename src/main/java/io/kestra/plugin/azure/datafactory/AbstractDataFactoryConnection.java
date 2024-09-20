package io.kestra.plugin.azure.datafactory;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.*;
import com.azure.resourcemanager.datafactory.DataFactoryManager;
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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDataFactoryConnection extends Task implements AbstractDataFactoryConnectionInterface {
    @NotNull
    protected Property<String> subscriptionId;
    @NotNull
    protected Property<String> tenantId;

    @Builder.Default
    protected Property<String> clientId = Property.of("");
    @Builder.Default
    protected Property<String> clientSecret = Property.of("");
    @Builder.Default
    protected Property<String> pemCertificate = Property.of("");

    protected DataFactoryManager getDataFactoryManager(RunContext runContext) throws IllegalVariableEvaluationException {
        runContext.logger().info("Authenticating to Azure Data Factory");
        return DataFactoryManager.authenticate(credentials(runContext), profile(runContext));
    }

    private AzureProfile profile(RunContext runContext) throws IllegalVariableEvaluationException {
        final String tenantId = this.tenantId.as(runContext, String.class);
        final String subscriptionId = this.subscriptionId.as(runContext, String.class);

        return  new AzureProfile(
                tenantId,
                subscriptionId,
                AzureEnvironment.AZURE
        );
    }

    private TokenCredential credentials(RunContext runContext) throws IllegalVariableEvaluationException {
        final String tenantId = runContext.render(this.tenantId.as(runContext, String.class));
        final String clientId = runContext.render(this.clientId.as(runContext, String.class));

        //Create client/secret credentials
        final String clientSecret = runContext.render(this.clientSecret.as(runContext, String.class));
        if(StringUtils.isNotBlank(clientSecret)) {
            return getClientSecretCredential(tenantId, clientId, clientSecret);
        }

        //Create client/certificate credentials
        final String pemCertificate = runContext.render(this.pemCertificate.as(runContext, String.class));
        if(StringUtils.isNotBlank(pemCertificate)) {
            return getClientCertificateCredential(tenantId, clientId, pemCertificate);
        }

        //Create default authentication
        return new DefaultAzureCredentialBuilder().tenantId(tenantId).build();
    }

    private ClientCertificateCredential getClientCertificateCredential(String clientId, String tenantId, String pemCertificate) {
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
