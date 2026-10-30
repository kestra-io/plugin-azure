# Microsoft Sentinel contributor notes

## Opt-in live validation

`SentinelLiveTest` is skipped unless `AZURE_SENTINEL_LIVE_TEST=true`. It uses the existing Azure service-principal environment convention (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`) and requires explicit `AZURE_SENTINEL_SUBSCRIPTION_ID`, `AZURE_SENTINEL_RESOURCE_GROUP`, `AZURE_SENTINEL_WORKSPACE_NAME`, `AZURE_SENTINEL_WORKSPACE_ID`, and `AZURE_SENTINEL_INCIDENT_ID`. Supply a dedicated disposable Sentinel-enabled workspace and an existing test incident. The live tests only read the configured incident/comments, list incidents, and issue a constant KQL query; they do not create or provision Azure resources.

Run `./gradlew test --tests '*sentinel.SentinelLiveTest'` in that configured environment. The normal `.github/setup-unit.sh` setup can still configure the rest of the suite; Sentinel's explicit variables are read directly and do not need to be written into a test configuration file. Missing required variables cause an opted-in test to fail. Mocked tests cover writes, pagination, conflicts, storage, and trigger state independently of Azure credentials.

## API contracts and review decisions

### Query timeouts

Azure Monitor's query timeout defaults to three minutes and cannot exceed ten minutes. The SDK exposes this setting through `LogsQueryOptions.setServerTimeout`. The plugin's `serverTimeout` configures that query option independently of the inherited Kestra `Task.timeout`, which controls task execution. A task timeout above ten minutes must remain valid when the server timeout is within Azure's limit.

Sources: [Azure Monitor timeout contract](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/api/timeouts), [Azure Java SDK options](https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/monitor/azure-monitor-query-logs/src/main/java/com/azure/monitor/query/logs/models/LogsQueryOptions.java), [Kestra Task](https://github.com/kestra-io/kestra/blob/develop/core/src/main/java/io/kestra/core/models/tasks/Task.java).

### Incident ownership

Azure's owner object includes `objectId`, `email`, `userPrincipalName`, and `assignedTo`. Retaining an old assignee's identifiers alongside new owner inputs creates inconsistent identity information. The review policy therefore replaces the owner object with the supplied owner fields whenever an owner change is requested; omitted ownership preserves the existing owner. Prefer `ownerObjectId` for assignment. Mocked payload tests establish replacement behavior, not live Azure acceptance of email-only assignment.

Sources: [Sentinel incident and owner schema](https://learn.microsoft.com/en-us/rest/api/securityinsights/incidents/list?view=rest-securityinsights-2024-09-01), [Maintainer review](https://github.com/kestra-io/plugin-azure/pull/358).

### Bounded incident polling

The incidents API supports `$filter`, `$orderby`, `$top` (at most 1000), and `nextLink` pagination. A bounded trigger uses an initial modification-time lookback, then consumes incidents in ascending modification-time order and stops once its fresh-incident limit is reached. The standalone listing task can retain descending order and complete pagination.

Only emitted incidents advance the watermark. An inclusive modification-time boundary and identity/time/ETag deduplication preserve equal-timestamp incidents across pages and restarts. Unchanged boundary records do not consume the fresh-incident limit; failures must not commit new state. These are plugin policies built on the API's filtering and ordering capabilities.

Watermark and boundary version timestamps are normalized to microseconds before comparison and persistence, matching Kestra 2.0 JSON serialization. Azure timestamps with seven or more fractional digits therefore remain stable across KV round trips and restarts. Identity and ETag still distinguish incidents and updates within the same microsecond. Existing six-digit KV state remains compatible; original timestamps in emitted incident payloads and the exact initial-lookback cutoff are preserved.

The incident cap bounds each execution's inline payload. It does not bound worst-case page scanning when many unchanged incidents share the watermark timestamp. Polling observes returned incident states and cannot guarantee exactly-once delivery or capture every intermediate edit.

Sources: [Sentinel pagination and ordering contract](https://learn.microsoft.com/en-us/rest/api/securityinsights/incidents/list?view=rest-securityinsights-2024-09-01), [Maintainer review](https://github.com/kestra-io/plugin-azure/pull/358).

## Icon provenance

The Sentinel subgroup icon is Microsoft's unmodified `10248-icon-service-Azure-Sentinel.svg` from [Azure Architecture Center icons](https://learn.microsoft.com/en-us/azure/architecture/icons/), [Azure_Public_Service_Icons_V24.zip](https://arch-center.azureedge.net/icons/Azure_Public_Service_Icons_V24.zip), used to identify the Microsoft Sentinel integration.
