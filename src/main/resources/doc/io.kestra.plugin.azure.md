# How to use the Azure plugin

Tasks support service principal, certificate, `DefaultAzureCredential`, shared key, and SAS token authentication depending on the service.

## Authentication

All tasks must be authenticated for the Azure Platform. Multiple authentication methods are supported:

### 1. Service Principal with Client Secret
You can set the following task properties:
- `tenantId`: Directory (tenant) ID of the Azure Active Directory instance.
- `clientId`: Application (client) ID of your service principal.
- `clientSecret`: Secret associated with your service principal.

This is a common method for server-to-server authentication and recommended for automation scenarios. This is best used with [secrets](https://kestra.io/docs/concepts/secret) to avoid exposing credentials in plain text.

### 2. Service Principal with Certificate
Alternatively, you can use a PEM certificate for authentication by specifying:
- `tenantId`
- `clientId`
- `pemCertificate`: PEM-formatted certificate content.

This method is preferred over client secrets when enhanced security and certificate lifecycle management are required.

### 3. Default Azure Credentials
If no client secret or certificate is defined, the [DefaultAzureCredential](https://learn.microsoft.com/en-us/java/api/overview/azure/identity-readme?view=azure-java-stable#defaultazurecredential) chain will be used. This includes:
- Environment variables (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, etc.).
- Managed identity for Azure resources (if the task is running on an Azure VM, App Service, etc.).
- Azure CLI logged-in user.
- Visual Studio Code or Azure Developer CLI credentials.

> ⚠️ In all cases, specifying `tenantId` is **required**.

### 4. SAS Token or Shared Key Authentication
Some Azure services support alternate authentication modes:
- **Shared Key**: use `sharedKeyAccountName` and `sharedKeyAccountAccessKey` for services like Azure Storage.
- **SAS Token**: use `sasToken` for temporary delegated access to resources.

These can also be stored as [secrets](https://kestra.io/docs/concepts/secret).

## Common properties

Most tasks require an `endpoint` property pointing to the Azure service endpoint (e.g., a Blob storage URL). Some tasks accept a `scopes` property to override the default OAuth scope (`https://management.azure.com/.default`).

## Tasks

Tasks span the most commonly used Azure services. The `storage.blob` and `storage.adls` packages cover uploads, downloads, copies, deletions, and file-arrival triggers for Blob Storage and ADLS Gen2. For messaging, `eventhubs` and `servicebus` each offer produce, consume, a polling `Trigger`, and a `RealtimeTrigger` — use `Trigger` for batch processing on a schedule and `RealtimeTrigger` for per-message executions.

For data and compute, `datafactory` triggers pipeline runs, `synapse.SparkBatchJobCreate` submits Spark jobs, and `batch` manages HPC pools and jobs. `storage.cosmosdb` and `storage.table` cover NoSQL reads and writes, and `function.HttpFunction` invokes Azure Functions. Use `cli.AzCLI` for operations not covered by a dedicated task.

## Azure HorizonDB

`horizondb` connects to Azure HorizonDB, Microsoft's managed PostgreSQL-compatible service, over the standard PostgreSQL JDBC driver. Each task and trigger takes `host`, `port`, `database`, and either a `username`/`password` pair or `useEntraId: true`, which authenticates via the Azure Identity Extensions JDBC plugin rather than the Service Principal flow described above for the rest of this plugin. With `useEntraId: true` and no further properties set, it falls back to whatever `DefaultAzureCredential` resolves on the worker (managed identity, environment variables, Azure CLI login, etc.); set `tenantId`/`clientId`/`clientSecret` alongside it to authenticate as a specific service principal instead — the same three properties used for that purpose on `monitoring.Trigger` and the `servicebus` tasks. Connections default to `sslmode=require`; set `ssl: false` only for local, non-TLS development.

The `horizondb.durable` tasks and trigger wrap `pg_durable`, Microsoft's open-source durable-execution PostgreSQL extension that HorizonDB ships with. `pg_durable`'s `df.*` SQL function surface (`df.start`, `df.cancel`, `df.signal`, `df.status`, `df.result`, `df.list_instances`, and more) is publicly documented and independently verifiable at:
- Extension source and user guide: https://github.com/microsoft/pg_durable (see `USER_GUIDE.md`, in particular the "Quick Reference Card" and "Monitoring" sections for exact function signatures)
- HorizonDB-specific docs: https://learn.microsoft.com/en-us/azure/horizondb/development/durable-functions

- `horizondb.Query` / `horizondb.Queries` run one or more SQL statements, with `fetchType` controlling whether results are returned inline (`FETCH`, `FETCH_ONE`), streamed to internal storage (`STORE`), or discarded (`NONE`).
- `horizondb.durable.Start`, `Cancel`, `Signal`, `GetStatus`, and `ListInstances` manage `pg_durable` durable function instances directly from SQL (`df.start(func, label, database)`, `df.cancel(id, reason)`, `df.signal(id, name, data)`, `df.status`/`df.result`, and `df.list_instances(status, limit)`).
- `horizondb.durable.Trigger` polls `df.list_instances(status)` and starts an execution the first time an instance newly reaches a target status, without refiring for instances that remain in that status.
## Logic Apps

The `logicapps` package provides tasks and triggers for Azure Logic Apps workflows:
- `logicapps.Run` - trigger a workflow's trigger (e.g. `manual`) and return the run id / status.
- `logicapps.List` - list workflows in a resource group.
- `logicapps.ListRuns` - list recent workflow runs with optional status filtering.
- `logicapps.Get` - retrieve workflow metadata.
- `logicapps.GetRun` - retrieve a specific workflow run's details (status, outputs, errors).
- `logicapps.Trigger` - stateful polling trigger that starts Kestra executions for newly observed workflow runs matching configured statuses.

These support service principal and certificate authentication consistent with other Azure tasks. Use the `statusFilter` or `statuses` properties to scope runs, and the `Trigger` provides deduplication and state TTL controls.


## Azure AI Foundry

The `aifoundry` package provides tasks and a trigger for interacting with Azure AI Foundry:

- `aifoundry.ChatCompletion` - Call a deployed model for chat completions.
- `aifoundry.Embeddings` - Generate vector embeddings from text input.
- `aifoundry.RunAgent` - Create and run an Azure AI Foundry agent, returning the conversation result.
- `aifoundry.CreateEvaluation` - Submit a new evaluation job using a dataset and a set of evaluators.
- `aifoundry.GetDeployment` - Retrieve deployment status and configuration.
- `aifoundry.Trigger` - Poll Azure AI Foundry for newly completed evaluations and fire an execution. Use the `statuses` and `maxEvaluations` properties to scope evaluations; the trigger provides deduplication and state TTL controls.

### Authentication for Azure AI Foundry

- Tasks like `ChatCompletion` and `Embeddings` support API-key authentication (via the `apiKey` property) or Entra ID (`DefaultAzureCredential`).
- Tasks that use the Azure AI Projects SDK (like `RunAgent`, `CreateEvaluation`, `GetDeployment`, and `Trigger`) **require** Entra ID (`DefaultAzureCredential`) as API keys are not supported by the underlying client. Do not provide the `apiKey` property when using these tasks.
