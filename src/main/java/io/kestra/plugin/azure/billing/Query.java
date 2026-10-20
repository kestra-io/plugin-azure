package io.kestra.plugin.azure.billing;

import java.math.BigDecimal;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query Azure cost and usage with Cost Management",
    description = """
        Calls the Cost Management `Query - Usage` API on a scope and returns one row per period and group.
        The API is free but rate limited per scope, so request only the columns and groupings you need and keep
        `maxPages` low."""
)
@Plugin(
    examples = {
        @Example(
            title = "Get last month's actual cost per resource group and log the total.",
            full = true,
            code = """
                id: azure_monthly_cost_by_rg
                namespace: company.finops

                tasks:
                  - id: cost
                    type: io.kestra.plugin.azure.billing.Query
                    scope: "/subscriptions/{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    costType: ActualCost
                    timeframe: TheLastMonth
                    granularity: None
                    aggregations:
                      - Cost
                    groupBy:
                      - ResourceGroupName
                    fetchType: FETCH

                  - id: log_total
                    type: io.kestra.plugin.core.log.Log
                    message: "Last month: {{ outputs.cost.costTotals.Cost }} {{ outputs.cost.unit }} over {{ outputs.cost.size }} resource groups"

                pluginDefaults:
                  - type: io.kestra.plugin.azure
                    values:
                      tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                      clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                      clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                """
        ),
        @Example(
            title = "Store the daily cost of a custom period for one service, grouped by a tag.",
            full = true,
            code = """
                id: azure_daily_cost_team
                namespace: company.finops

                tasks:
                  - id: cost
                    type: io.kestra.plugin.azure.billing.Query
                    scope: "/subscriptions/{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    costType: AmortizedCost
                    timeframe: Custom
                    startDate: "{{ now() | dateAdd(-7, 'DAYS') | date('yyyy-MM-dd') }}"
                    endDate: "{{ now() | dateAdd(-1, 'DAYS') | date('yyyy-MM-dd') }}"
                    granularity: Daily
                    aggregations:
                      - Cost
                    groupBy:
                      - "TAG:team"
                    dimensionFilter:
                      ServiceName:
                        - Storage

                pluginDefaults:
                  - type: io.kestra.plugin.azure
                    values:
                      tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                      clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                      clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                """
        )
    },
    metrics = {
        @Metric(name = "requests", type = Counter.TYPE, unit = "requests", description = "Number of Cost Management requests made."),
        @Metric(name = "fetch.rows", type = Counter.TYPE, unit = "rows", description = "Number of rows fetched or stored.")
    }
)
public class Query extends AbstractBilling implements RunnableTask<Query.Output> {
    private static final int MAX_GROUP_BY = 2;

    @Schema(title = "Cost type", description = "Required. `ActualCost`, `AmortizedCost` or `Usage`.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<CostQueries.CostType> costType;

    @Schema(
        title = "Timeframe",
        description = "`Custom` requires `startDate` and `endDate`. The `Billing*` timeframes are only meaningful at EA/MCA billing scopes."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Timeframe> timeframe;

    @Schema(title = "Start date", description = "First day of a `Custom` timeframe, inclusive, as `yyyy-MM-dd`.")
    @PluginProperty(group = "main")
    private Property<String> startDate;

    @Schema(
        title = "End date",
        description = "Last day of a `Custom` timeframe, as `yyyy-MM-dd`. **Inclusive**, like in the Cost Management API: " +
            "the whole month of January is `2026-01-01` to `2026-01-31`."
    )
    @PluginProperty(group = "main")
    private Property<String> endDate;

    @Schema(title = "Granularity", description = "`Daily`, `Monthly`, or `None` for a single total per group.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<Granularity> granularity;

    @Schema(
        title = "Aggregation columns",
        description = "Required. Cost columns to sum, for example `Cost`, `PreTaxCost`, `CostUSD` or `UsageQuantity`. At least one column must be given."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<String>> aggregations;

    @Schema(
        title = "Group by",
        description = "Up to 2 groupings. A plain value is a dimension (e.g. `ResourceGroupName`, `ServiceName`, `ResourceLocation`, " +
            "`SubscriptionName`), `TAG:<key>` groups by a tag."
    )
    @PluginProperty(group = "processing")
    private Property<List<String>> groupBy;

    @Schema(
        title = "Filter",
        description = "Only include costs matching all entries (entries are ANDed). Keys use the same syntax as `groupBy` (`ServiceName`, `TAG:team`); " +
            "the values listed for one key are ORed, so a cost matches when its value is any of them."
    )
    @PluginProperty(group = "processing")
    private Property<Map<String, List<String>>> dimensionFilter;

    @Schema(
        title = "Maximum number of pages",
        description = "Stops after this many requests (following `nextLink`) and logs a warning if more rows were available."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<@Min(1) Integer> maxPages = Property.ofValue(DEFAULT_MAX_PAGES);

    @Schema(
        title = "Fetch strategy",
        description = "Default STORE writes all rows to internal storage. FETCH returns all rows, FETCH_ONE the first row, NONE only the totals."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rScope = renderScope(runContext);
        var rAggregations = runContext.render(aggregations).asList(String.class);
        var rGroupBy = runContext.render(groupBy).asList(String.class);
        if (rGroupBy.size() > MAX_GROUP_BY) {
            throw new IllegalArgumentException("Cost Management supports at most " + MAX_GROUP_BY + " groupBy entries, got " + rGroupBy.size());
        }
        var rTimeframe = runContext.render(timeframe).as(Timeframe.class).orElseThrow(() -> new IllegalArgumentException("timeframe is required"));
        var rStart = runContext.render(startDate).as(String.class);
        var rEnd = runContext.render(endDate).as(String.class);
        var rMaxPages = renderMaxPages(runContext, maxPages);

        var dataset = new LinkedHashMap<String, Object>();
        dataset.put("granularity", runContext.render(granularity).as(Granularity.class).orElseThrow(() -> new IllegalArgumentException("granularity is required")).name());
        dataset.put("aggregation", CostQueries.aggregation(rAggregations));
        if (!rGroupBy.isEmpty()) {
            dataset.put("grouping", rGroupBy.stream().map(CostQueries::grouping).toList());
        }
        var filter = CostQueries.filter(runContext.render(dimensionFilter).asMap(String.class, List.class));
        if (filter != null) {
            dataset.put("filter", filter);
        }

        var body = new LinkedHashMap<String, Object>();
        body.put("type", runContext.render(costType).as(CostQueries.CostType.class).orElseThrow(() -> new IllegalArgumentException("costType is required")).name());
        body.put("timeframe", rTimeframe.name());
        if (rTimeframe == Timeframe.Custom) {
            if (rStart.isEmpty() || rEnd.isEmpty()) {
                throw new IllegalArgumentException("startDate and endDate are required when timeframe is Custom");
            }
            body.put("timePeriod", CostQueries.timePeriod(parseDate("startDate", rStart.get()), parseDate("endDate", rEnd.get())));
        } else if (rStart.isPresent() || rEnd.isPresent()) {
            throw new IllegalArgumentException("startDate and endDate can only be set when timeframe is Custom, got " + rTimeframe);
        }
        body.put("dataset", dataset);

        var totals = new LinkedHashMap<String, BigDecimal>();
        rAggregations.forEach(column -> totals.put(column, BigDecimal.ZERO));
        var totalsByCurrency = new LinkedHashMap<String, Map<String, BigDecimal>>();
        String url = MANAGEMENT_ENDPOINT + rScope + "/providers/Microsoft.CostManagement/query?api-version=" + CostQueries.API_VERSION;
        int pages = 0;

        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.STORE);
        BillingFetch.Result fetched;
        try (var api = api(runContext); var fetch = BillingFetch.open(runContext, rFetchType)) {
            while (url != null && pages < rMaxPages) {
                var response = api.send("POST", url, body);
                pages++;
                for (var row : CostQueries.rows(response)) {
                    var currency = row.get("Currency") == null ? null : row.get("Currency").toString();
                    for (var column : rAggregations) {
                        if (row.get(column) != null) {
                            var amount = CostQueries.decimal(row.get(column));
                            totals.merge(column, amount, BigDecimal::add);
                            if (currency != null) {
                                totalsByCurrency.computeIfAbsent(currency, c -> new LinkedHashMap<>()).merge(column, amount, BigDecimal::add);
                            }
                        }
                    }
                    fetch.add(row);
                }
                url = CostQueries.nextLink(response);
            }
            fetched = fetch.finish();
        }

        runContext.metric(Counter.of("requests", pages));
        if (url != null) {
            runContext.logger().warn("Stopped after maxPages={} Cost Management requests, more rows are available. Increase maxPages or narrow the query.", rMaxPages);
        }

        return Output.builder()
            .rows(fetched.rows())
            .row(fetched.row())
            .uri(fetched.uri())
            .size(fetched.size())
            .costTotals(totalsByCurrency.size() > 1 ? null : totals)
            .costTotalsByCurrency(totalsByCurrency)
            .unit(totalsByCurrency.size() == 1 ? totalsByCurrency.keySet().iterator().next() : null)
            .truncated(url != null)
            .build();
    }

    public enum Timeframe {
        MonthToDate,
        BillingMonthToDate,
        TheLastMonth,
        TheLastBillingMonth,
        WeekToDate,
        Custom
    }

    public enum Granularity {
        Daily,
        Monthly,
        None
    }

    @Builder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Rows",
            description = "All rows when fetchType is FETCH, keyed by the column names returned by Azure, e.g. `Cost`, `UsageDate`, " +
                "`ResourceGroupName`, `Currency`."
        )
        private List<Map<String, Object>> rows;

        @Schema(title = "First row", description = "Single row when fetchType is FETCH_ONE.")
        private Map<String, Object> row;

        @Schema(title = "Stored data URI", description = "Internal storage URI of an ION file with all rows when fetchType is STORE.")
        private URI uri;

        @Schema(title = "Row count", description = "Number of rows returned by Cost Management.")
        private Long size;

        @Schema(
            title = "Cost totals",
            description = "Sum of each aggregation column over all returned rows, keyed by column name (e.g. `costTotals.Cost`). " +
                "Null when the rows are in more than one currency (possible at EA, MCA or CSP billing scopes): use `costTotalsByCurrency` then."
        )
        private Map<String, BigDecimal> costTotals;

        @Schema(
            title = "Cost totals by currency",
            description = "Sum of each aggregation column per value of the `Currency` column, e.g. `costTotalsByCurrency.EUR.Cost`. " +
                "Empty when the response has no `Currency` column."
        )
        private Map<String, Map<String, BigDecimal>> costTotalsByCurrency;

        @Schema(
            title = "Currency",
            description = "Currency of the costs, from the `Currency` column, e.g. `USD` or `EUR`. Null when the rows are in more than one currency."
        )
        private String unit;

        @Schema(title = "Truncated", description = "True when `maxPages` was reached before all rows were read.")
        private Boolean truncated;
    }
}
