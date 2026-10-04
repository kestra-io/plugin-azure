package io.kestra.plugin.azure.sentinel;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.Context;
import com.azure.monitor.query.logs.LogsQueryClient;
import com.azure.monitor.query.logs.LogsQueryClientBuilder;
import com.azure.monitor.query.logs.models.*;

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
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query Microsoft Sentinel logs with KQL",
    description = "Queries the Log Analytics workspace backing Sentinel. Returns the primary result table with column metadata. Partial query errors fail the task. The Azure SDK may materialize the response before results are stored."
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
        timeout: PT3M
        fetchType: STORE
    """))
public class Query extends AbstractAzureIdentityConnection implements RunnableTask<Query.Output> {
    @NotNull
    @Schema(title = "Log Analytics workspace ID", description = "The workspace customer ID (GUID), not its ARM resource ID or workspace name.")
    @PluginProperty(group = "connection")
    private Property<String> workspaceId;

    @NotNull
    @Schema(title = "KQL query", description = "Kusto Query Language expression executed against the workspace.")
    @PluginProperty(group = "main")
    private Property<String> query;

    @Builder.Default
    @Schema(title = "Query timespan", description = "Positive ISO-8601 look-back duration. Defaults to P1D.")
    @PluginProperty(group = "main")
    private Property<Duration> timespan = Property.ofValue(Duration.ofDays(1));

    @Builder.Default
    @Schema(title = "Server timeout", description = "Positive ISO-8601 duration, at most PT10M. Defaults to PT3M.")
    @PluginProperty(group = "advanced")
    private Property<Duration> timeout = Property.ofValue(Duration.ofMinutes(3));

    @Builder.Default
    @NotNull
    @Schema(title = "Result fetching mode", description = "STORE writes ION to internal storage, FETCH returns all rows, and FETCH_ONE returns the first row. Other modes are unsupported.")
    @PluginProperty(group = "main")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    protected LogsQueryClient queryClient(RunContext runContext) throws Exception {
        return clientBuilder(runContext).addPolicy(columnMetadataPolicy()).buildClient();
    }

    protected LogsQueryClientBuilder clientBuilder(RunContext runContext) throws Exception {
        return new LogsQueryClientBuilder().credential(credentials(runContext));
    }

    private static final String COLUMN_METADATA = Query.class.getName() + ".columns";

    // SDK 1.0.6 discards the REST column definitions when mapping LogsTable, including for empty results.
    // Capture the primary table schema in the per-request context before the SDK maps the buffered response.
    private static HttpPipelinePolicy columnMetadataPolicy() {
        return (context, next) -> next.process().flatMap(response ->
        {
            var target = context.getContext().getData(COLUMN_METADATA);
            if (response.getStatusCode() != 200 || target.isEmpty()) {
                return reactor.core.publisher.Mono.just(response);
            }
            var buffered = response.buffer();
            return buffered.getBodyAsString().flatMap(body -> reactor.core.publisher.Mono.fromCallable(() ->
            {
                var tables = JacksonMapper.ofJson().readTree(body).path("tables");
                List<Column> columns = new ArrayList<>();
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
        String workspace = runContext.render(workspaceId).as(String.class).filter(s -> !s.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("workspaceId is required"));
        String kql = runContext.render(query).as(String.class).filter(s -> !s.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("query is required"));
        Duration span = runContext.render(timespan).as(Duration.class).orElse(Duration.ofDays(1));
        Duration serverTimeout = runContext.render(timeout).as(Duration.class).orElse(Duration.ofMinutes(3));
        FetchType mode = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.STORE);
        if (span.isZero() || span.isNegative()) {
            throw new IllegalArgumentException("timespan must be positive");
        }
        if (serverTimeout.isZero() || serverTimeout.isNegative() || serverTimeout.compareTo(Duration.ofMinutes(10)) > 0) {
            throw new IllegalArgumentException("timeout must be positive and at most PT10M");
        }
        if (mode != FetchType.FETCH && mode != FetchType.FETCH_ONE && mode != FetchType.STORE) {
            throw new IllegalArgumentException("fetchType must be FETCH, FETCH_ONE or STORE");
        }

        AtomicReference<List<Column>> metadata = new AtomicReference<>();
        LogsQueryResult result = queryClient(runContext).queryWorkspaceWithResponse(
            workspace, kql, new LogsQueryTimeInterval(span),
            new LogsQueryOptions().setServerTimeout(serverTimeout), new Context(COLUMN_METADATA, metadata)
        ).getValue();
        if (result.getError() != null || result.getQueryResultStatus() != LogsQueryResultStatus.SUCCESS) {
            throw new IllegalStateException(
                "Azure Logs query failed: " + (result.getError() == null
                    ? result.getQueryResultStatus()
                    : result.getError().getCode() + ": " + result.getError().getMessage())
            );
        }
        LogsTable table = result.getAllTables() == null || result.getAllTables().isEmpty() ? null : result.getAllTables().getFirst();
        List<Column> columns = metadata.get();
        if (columns == null) {
            throw new IllegalStateException("Azure Logs response did not include column metadata");
        }
        List<LogsTableRow> records = table == null ? List.of() : table.getRows();
        var output = Output.builder().columns(columns);
        if (mode == FetchType.FETCH_ONE) {
            return output.row(records.isEmpty() ? null : normalize(records.getFirst()))
                .count(records.isEmpty() ? 0L : 1L).build();
        }
        if (mode == FetchType.FETCH) {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (LogsTableRow record : records) {
                rows.add(normalize(record));
            }
            return output.rows(rows).count((long) rows.size()).build();
        }
        File file = runContext.workingDir().createTempFile(".ion").toFile();
        try (var stream = new BufferedOutputStream(new FileOutputStream(file))) {
            for (LogsTableRow record : records) {
                FileSerde.write(stream, normalize(record));
            }
        }
        return output.uri(runContext.storage().putFile(file)).count((long) records.size()).build();
    }

    private static Map<String, Object> normalize(LogsTableRow row) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        for (LogsTableCell cell : row.getRow()) {
            String text = cell.getValueAsString();
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
        @Schema(title = "All rows", description = "Present for FETCH.")
        private final List<Map<String, Object>> rows;
        @Schema(title = "First row", description = "Present for nonempty FETCH_ONE results.")
        private final Map<String, Object> row;
        @Schema(title = "Stored ION URI", description = "Present for STORE.")
        private final URI uri;
        @Schema(title = "Number of returned or stored rows")
        private final Long count;
    }
}
