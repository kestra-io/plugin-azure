### Authentication

All tasks must be authenticated for the Azure Platform. Multiple authentication methods are supported:

#### 1. Service Principal with Client Secret
You can set the following task properties:
- `tenantId`: Directory (tenant) ID of the Azure Active Directory instance.
- `clientId`: Application (client) ID of your service principal.
- `clientSecret`: Secret associated with your service principal.

This is a common method for server-to-server authentication and recommended for automation scenarios. This is best used with [secrets](https://kestra.io/docs/concepts/secret) to avoid exposing credentials in plain text.

#### 2. Service Principal with Certificate
Alternatively, you can use a PEM certificate for authentication by specifying:
- `tenantId`
- `clientId`
- `pemCertificate`: PEM-formatted certificate content.

This method is preferred over client secrets when enhanced security and certificate lifecycle management are required.

#### 3. **Default Azure Credentials**
If no client secret or certificate is defined, the [DefaultAzureCredential](https://learn.microsoft.com/en-us/java/api/overview/azure/identity-readme?view=azure-java-stable#defaultazurecredential) chain will be used. This includes:
- Environment variables (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, etc.).
- Managed identity for Azure resources (if the task is running on an Azure VM, App Service, etc.).
- Azure CLI logged-in user.
- Visual Studio Code or Azure Developer CLI credentials.

> ⚠️ In all cases, specifying `tenantId` is **required**.

#### 4. SAS Token or Shared Key Authentication
Some Azure services support alternate authentication modes:
- **Shared Key**: use `sharedKeyAccountName` and `sharedKeyAccountAccessKey` for services like Azure Storage.
- **SAS Token**: use `sasToken` for temporary delegated access to resources.

These can also be stored as [secrets](https://kestra.io/docs/concepts/secret).

---

### Common Properties

- `endpoint`: Most tasks require an `endpoint` property pointing to the Azure service endpoint (e.g., Blob storage URL).
- `scopes`: Some tasks allow you to define custom scopes (defaults to `https://management.azure.com/.default`).

---

### Example

```yaml
id: azure_get_token
namespace: company.team

tasks:
  - id: get_access_token
    type: io.kestra.plugin.azure.oauth.OauthAccessToken
    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
```

For more information on Azure authentication, see [Azure Identity documentation](https://learn.microsoft.com/en-us/java/api/overview/azure/identity-readme?view=azure-java-stable).
