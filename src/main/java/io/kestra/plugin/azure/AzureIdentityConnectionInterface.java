package io.kestra.plugin.azure;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

public interface AzureIdentityConnectionInterface {
    @Schema(
            title = "Client ID",
            description = """
                    Client ID of the Azure service principal.
                    If you don't have a service principal, refer to [create a service principal with Azure CLI](https://learn.microsoft.com/en-us/cli/azure/azure-cli-sp-tutorial-1?tabs=bash).
                    """
    )
    Property<String> getClientId();

    @Schema(
            title = "Client Secret",
            description = """
                    Service principal client secret.
                    The tenantId, clientId and clientSecret of the service principal are required for this credential to acquire an access token.
                    """
    )
    Property<String> getClientSecret();

    @Schema(
            title = "PEM Certificate",
            description = """
                Your stored PEM certificate.
                The tenantId, clientId and clientCertificate of the service principal are required for this credential to acquire an access token.
                """
    )
    Property<String> getPemCertificate();

    @Schema(title = "Tenant ID")
    Property<String> getTenantId();
}
