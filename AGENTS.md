# Kestra Azure Plugin

## What

- Provides plugin components under `io.kestra.plugin.azure`.
- Includes classes such as `Message`, `Consume`, `RealTimeTrigger`, `SerdeType`.

## Why

- What user problem does this solve? Teams need to leverage Microsoft Azure services within Kestra data workflows from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Azure steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Azure.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `azure`

Infrastructure dependencies (Docker Compose services):

- `app`

### Key Plugin Classes

- `io.kestra.plugin.azure.auth.OauthAccessToken`
- `io.kestra.plugin.azure.batch.job.Create`
- `io.kestra.plugin.azure.batch.pool.Resize`
- `io.kestra.plugin.azure.cli.AzCLI`
- `io.kestra.plugin.azure.datafactory.CreateRun`
- `io.kestra.plugin.azure.eventhubs.Consume`
- `io.kestra.plugin.azure.eventhubs.Produce`
- `io.kestra.plugin.azure.eventhubs.RealtimeTrigger`
- `io.kestra.plugin.azure.eventhubs.Trigger`
- `io.kestra.plugin.azure.function.HttpFunction`
- `io.kestra.plugin.azure.monitoring.Push`
- `io.kestra.plugin.azure.monitoring.Query`
- `io.kestra.plugin.azure.monitoring.Trigger`
- `io.kestra.plugin.azure.servicebus.Consume`
- `io.kestra.plugin.azure.servicebus.Publish`
- `io.kestra.plugin.azure.servicebus.RealTimeTrigger`
- `io.kestra.plugin.azure.servicebus.Trigger`
- `io.kestra.plugin.azure.storage.adls.Delete`
- `io.kestra.plugin.azure.storage.adls.DeleteFiles`
- `io.kestra.plugin.azure.storage.adls.List`
- `io.kestra.plugin.azure.storage.adls.Read`
- `io.kestra.plugin.azure.storage.adls.Reads`
- `io.kestra.plugin.azure.storage.adls.SharedAccess`
- `io.kestra.plugin.azure.storage.adls.Trigger`
- `io.kestra.plugin.azure.storage.adls.Upload`
- `io.kestra.plugin.azure.storage.adls.update.Append`
- `io.kestra.plugin.azure.storage.adls.update.Lease`
- `io.kestra.plugin.azure.storage.adls.update.SetAccessControl`
- `io.kestra.plugin.azure.storage.blob.Copy`
- `io.kestra.plugin.azure.storage.blob.Delete`
- `io.kestra.plugin.azure.storage.blob.DeleteList`
- `io.kestra.plugin.azure.storage.blob.Download`
- `io.kestra.plugin.azure.storage.blob.Downloads`
- `io.kestra.plugin.azure.storage.blob.List`
- `io.kestra.plugin.azure.storage.blob.SharedAccess`
- `io.kestra.plugin.azure.storage.blob.Trigger`
- `io.kestra.plugin.azure.storage.blob.Upload`
- `io.kestra.plugin.azure.storage.cosmosdb.Batch`
- `io.kestra.plugin.azure.storage.cosmosdb.CreateItem`
- `io.kestra.plugin.azure.storage.cosmosdb.Delete`
- `io.kestra.plugin.azure.storage.cosmosdb.Queries`
- `io.kestra.plugin.azure.storage.cosmosdb.Query`
- `io.kestra.plugin.azure.storage.table.Bulk`
- `io.kestra.plugin.azure.storage.table.Delete`
- `io.kestra.plugin.azure.storage.table.Get`
- `io.kestra.plugin.azure.storage.table.List`
- `io.kestra.plugin.azure.synapse.SparkBatchJobCreate`

### Project Structure

```
plugin-azure/
├── src/main/java/io/kestra/plugin/azure/synapse/
├── src/test/java/io/kestra/plugin/azure/synapse/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
