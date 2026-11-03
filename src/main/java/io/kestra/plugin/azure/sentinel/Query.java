package io.kestra.plugin.azure.sentinel;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.azure.core.http.HttpClient;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.Context;
import com.azure.monitor.query.logs.LogsQueryClientBuilder;
import com.azure.monitor.query.logs.models.LogsQueryOptions;
import com.azure.monitor.query.logs.models.LogsQueryResultStatus;
import com.azure.monitor.query.logs.models.LogsQueryTimeInterval;
import com.azure.monitor.query.logs.models.LogsTableRow;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.azure.shared.AbstractAzureIdentityConnection;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Mono;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query Microsoft Sentinel logs with KQL",
    description = """
        Queries the Log Analytics workspace backing Sentinel. Returns the primary result table with column metadata.
        Partial query errors fail the task. The Azure SDK may materialize the response before results are stored."""
)
@Plugin(examples = @Example(title = "Store recent security alerts", full = true, code = """
    id: sentinel_query
    namespace: company.team
    tasks:
      - id: query
        type: io.kestra.plugin.azure.sentinel.Query
        tenantId: "{{ secret('AZURE_TENANT_ID') }}"
        clientId: "{{ secret('AZURE_CLIENT_ID') }}"
        clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
        workspaceId: "{{ secret('AZURE_LOG_ANALYTICS_WORKSPACE_ID') }}"
        query: SecurityAlert | take 100
        timespan: P1D
        serverTimeout: PT3M
        fetchType: STORE
    """))
public class Query extends AbstractAzureIdentityConnection implements RunnableTask<Query.Output> {
    static final String DEFAULT_LOG_ANALYTICS_ENDPOINT = "https://api.loganalytics.io";

    @NotNull
    @Schema(
        title = "Log Analytics workspace ID",
        description = """
            The workspace customer ID (GUID), not its ARM resource ID or workspace name."""
    )
    @PluginProperty(group = "connection")
    private Property<String> workspaceId;

    @Builder.Default
    @Schema(
        title = "Log Analytics endpoint",
        description = """
            Base URL of the Log Analytics query API. Defaults to `https://api.loganalytics.io` (public cloud).
            Override it for sovereign clouds, for example `https://api.loganalytics.us` (Azure Government) or
            `https://api.loganalytics.azure.cn` (Azure China). The access token scope is derived
            from this endpoint. Microsoft Entra sign-in uses the public authority unless the worker sets the
            `AZURE_AUTHORITY_HOST` environment variable, for example `https://login.microsoftonline.us`."""
    )
    @PluginProperty(group = "connection")
    private Property<String> logAnalyticsEndpoint = Property.ofValue(DEFAULT_LOG_ANALYTICS_ENDPOINT);

    @NotNull
    @Schema(
        title = "KQL query",
        description = """
            Kusto Query Language expression executed against the workspace."""
    )
    @PluginProperty(group = "main")
    private Property<String> query;

    @Builder.Default
    @Schema(
        title = "Query timespan",
        description = """
            Positive ISO-8601 look-back duration. Defaults to P1D."""
    )
    @PluginProperty(group = "main")
    private Property<Duration> timespan = Property.ofValue(Duration.ofDays(1));

    @Builder.Default
    @Schema(
        title = "Server timeout",
        description = """
            ISO-8601 duration between PT1S and PT10M. Defaults to PT3M. Azure takes whole seconds."""
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> serverTimeout = Property.ofValue(Duration.ofMinutes(3));

    @Builder.Default
    @NotNull
    @Schema(
        title = "Result fetching mode",
        description = """
            STORE writes ION to internal storage, FETCH returns all rows, and FETCH_ONE returns the first row. Other modes are unsupported."""
    )
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    private static final String COLUMN_METADATA = Query.class.getName() + ".columns";

    // SDK 1.0.6 drops REST column definitions (even for empty results), so capture them before mapping.
    private static HttpPipelinePolicy columnMetadataPolicy() {
        return (context, next) -> next.process().flatMap(response ->
        {
            var target = context.getContext().getData(COLUMN_METADATA);
            if (response.getStatusCode() != 200 || target.isEmpty()) {
                return Mono.just(response);
            }
            var buffered = response.buffer();
            return buffered.getBodyAsString().flatMap(body -> Mono.fromCallable(() ->
            {
                var tables = JacksonMapper.ofJson().readTree(body).path("tables");
                var columns = new ArrayList<Column>();
                if (tables.isArray() && !tables.isEmpty()) {
                    for (var column : tables.get(0).path("columns")) {
                        columns.add(new Column(column.path("name").asText(), column.path("type").asText()));
                    }
                }
                @SuppressWarnings("unchecked")
                var metadata = (AtomicReference<List<Column>>) target.get();
                metadata.set(columns);
                return buffered;
            }));
        });
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return run(runContext, null);
    }

    // Test seam: lets tests supply an HttpClient that trusts a self-signed certificate.
    Output run(RunContext runContext, HttpClient httpClient) throws Exception {
        var rWorkspaceId = runContext.render(workspaceId).as(String.class).filter(s -> !s.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("workspaceId is required"));
        var rQuery = runContext.render(query).as(String.class).filter(s -> !s.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("query is required"));
        var rTimespan = runContext.render(timespan).as(Duration.class).orElse(Duration.ofDays(1));
        var rServerTimeout = runContext.render(serverTimeout).as(Duration.class).orElse(Duration.ofMinutes(3));
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.STORE);
        var rEndpoint = AbstractSentinel.endpoint(runContext, logAnalyticsEndpoint, DEFAULT_LOG_ANALYTICS_ENDPOINT);
        if (rTimespan.isZero() || rTimespan.isNegative()) {
            throw new IllegalArgumentException("timespan must be positive");
        }
        if (rServerTimeout.compareTo(Duration.ofSeconds(1)) < 0 || rServerTimeout.compareTo(Duration.ofMinutes(10)) > 0) {
            throw new IllegalArgumentException("serverTimeout must be between PT1S and PT10M");
        }
        if (rFetchType != FetchType.FETCH && rFetchType != FetchType.FETCH_ONE && rFetchType != FetchType.STORE) {
            throw new IllegalArgumentException("fetchType must be FETCH, FETCH_ONE or STORE");
        }

        var metadata = new AtomicReference<List<Column>>();
        var builder = new LogsQueryClientBuilder().credential(credentials(runContext)).endpoint(rEndpoint)
            .addPolicy(columnMetadataPolicy());
        if (httpClient != null) {
            builder.httpClient(httpClient);
        }
        var client = builder.buildClient();
        var result = client.queryWorkspaceWithResponse(
            rWorkspaceId, rQuery, new LogsQueryTimeInterval(rTimespan),
            new LogsQueryOptions().setServerTimeout(rServerTimeout), new Context(COLUMN_METADATA, metadata)
        ).getValue();
        if (result.getError() != null || result.getQueryResultStatus() != LogsQueryResultStatus.SUCCESS) {
            throw new IllegalStateException(
                "Azure Logs query failed: " + (result.getError() == null
                    ? result.getQueryResultStatus()
                    : result.getError().getCode() + ": " + result.getError().getMessage())
            );
        }
        var table = result.getAllTables() == null || result.getAllTables().isEmpty() ? null : result.getAllTables().getFirst();
        var columns = metadata.get();
        if (columns == null) {
            throw new IllegalStateException("Azure Logs response did not include column metadata");
        }
        var records = table == null ? List.<LogsTableRow> of() : table.getRows();
        var output = Output.builder().columns(columns);
        if (rFetchType == FetchType.FETCH_ONE) {
            return output.row(records.isEmpty() ? null : normalize(records.getFirst()))
                .count(records.isEmpty() ? 0L : 1L).build();
        }
        if (rFetchType == FetchType.FETCH) {
            var rows = new ArrayList<Map<String, Object>>();
            for (var record : records) {
                rows.add(normalize(record));
            }
            return output.rows(rows).count((long) rows.size()).build();
        }
        var file = runContext.workingDir().createTempFile(".ion").toFile();
        try (var stream = new BufferedOutputStream(new FileOutputStream(file))) {
            for (var record : records) {
                FileSerde.write(stream, normalize(record));
            }
        }
        return output.uri(runContext.storage().putFile(file)).count((long) records.size()).build();
    }

    private static Map<String, Object> normalize(LogsTableRow row) throws Exception {
        var result = new LinkedHashMap<String, Object>();
        for (var cell : row.getRow()) {
            var text = cell.getValueAsString();
            Object value = text == null ? null : switch (cell.getColumnType().toString()) {
                case "bool" -> cell.getValueAsBoolean();
                case "int" -> cell.getValueAsInteger();
                case "long" -> cell.getValueAsLong();
                case "real" -> cell.getValueAsDouble();
                case "decimal" -> new BigDecimal(text);
                case "datetime" -> cell.getValueAsDateTime().toString();
                case "dynamic" -> JacksonMapper.ofJson().readValue(cell.getValueAsDynamic().toString(), Object.class);
                default -> text;
            };
            result.put(cell.getColumnName(), value);
        }
        return result;
    }

    @Getter
    @AllArgsConstructor
    public static class Column {
        @Schema(title = "Column name")
        private final String name;
        @Schema(title = "Kusto column type")
        private final String type;
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Primary result table columns")
        private final List<Column> columns;
        @Schema(
            title = "All rows",
            description = """
                Present for FETCH."""
        )
        private final List<Map<String, Object>> rows;
        @Schema(
            title = "First row",
            description = """
                Present for nonempty FETCH_ONE results."""
        )
        private final Map<String, Object> row;
        @Schema(
            title = "Stored ION URI",
            description = """
                Present for STORE."""
        )
        private final URI uri;
        @Schema(title = "Number of returned or stored rows")
        private final Long count;
    }
}
