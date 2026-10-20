package io.kestra.plugin.azure.billing;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

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
    title = "Forecast Azure spend with Cost Management",
    description = """
        Calls the Cost Management `Forecast - Usage` API on a scope for an explicit period and returns the daily
        forecast and its total. Forecast only supports a custom period with daily granularity, so there is no
        timeframe or granularity to choose."""
)
@Plugin(
    examples = {
        @Example(
            title = "Forecast the actual cost of the next 30 days and log it.",
            full = true,
            code = """
                id: azure_cost_forecast
                namespace: company.finops

                tasks:
                  - id: forecast
                    type: io.kestra.plugin.azure.billing.Forecast
                    scope: "/subscriptions/{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    costType: ActualCost
                    startDate: "{{ now() | date('yyyy-MM-dd') }}"
                    endDate: "{{ now() | dateAdd(30, 'DAYS') | date('yyyy-MM-dd') }}"
                    aggregation: Cost

                  - id: log_forecast
                    type: io.kestra.plugin.core.log.Log
                    message: "Forecast for the next 30 days: {{ outputs.forecast.totalAmount }} {{ outputs.forecast.unit }}"

                pluginDefaults:
                  - type: io.kestra.plugin.azure
                    values:
                      tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                      clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                      clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                """
        )
    }
)
public class Forecast extends AbstractBilling implements RunnableTask<Forecast.Output> {
    @Schema(title = "Cost type", description = "Required. `ActualCost`, `AmortizedCost` or `Usage`.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<CostQueries.CostType> costType;

    @Schema(title = "Start date", description = "First day of the forecast period, inclusive, as `yyyy-MM-dd`.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> startDate;

    @Schema(title = "End date", description = "Last day of the forecast period, **inclusive** like in the Cost Management API, as `yyyy-MM-dd`.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> endDate;

    @Schema(title = "Aggregation column", description = "Required. Cost column to forecast, for example `Cost` or `PreTaxCost`.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> aggregation;

    @Schema(
        title = "Include actual cost",
        description = "Also return the actual cost of the days of the period that are already over."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> includeActualCost = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rScope = renderScope(runContext);
        var rAggregation = runContext.render(aggregation).as(String.class).orElseThrow(() -> new IllegalArgumentException("aggregation is required"));

        var dataset = new LinkedHashMap<String, Object>();
        dataset.put("granularity", "Daily");
        dataset.put("aggregation", CostQueries.aggregation(List.of(rAggregation)));

        var body = new LinkedHashMap<String, Object>();
        body.put("type", runContext.render(costType).as(CostQueries.CostType.class).orElseThrow(() -> new IllegalArgumentException("costType is required")).name());
        body.put("timeframe", "Custom");
        body.put(
            "timePeriod", CostQueries.timePeriod(
                parseDate("startDate", runContext.render(startDate).as(String.class).orElseThrow(() -> new IllegalArgumentException("startDate is required"))),
                parseDate("endDate", runContext.render(endDate).as(String.class).orElseThrow(() -> new IllegalArgumentException("endDate is required")))
            )
        );
        body.put("dataset", dataset);
        body.put("includeActualCost", runContext.render(includeActualCost).as(Boolean.class).orElse(false));
        body.put("includeFreshPartialCost", false);

        var url = MANAGEMENT_ENDPOINT + rScope + "/providers/Microsoft.CostManagement/forecast?api-version=" + CostQueries.API_VERSION;
        Map<String, Object> response;
        try (var api = api(runContext)) {
            response = api.send("POST", url, body);
        }
        var forecast = CostQueries.rows(response);

        var total = BigDecimal.ZERO;
        var totalsByCurrency = new LinkedHashMap<String, BigDecimal>();
        for (var row : forecast) {
            var currency = row.get("Currency") == null ? null : row.get("Currency").toString();
            if (currency != null) {
                totalsByCurrency.putIfAbsent(currency, BigDecimal.ZERO);
            }
            if (!"Actual".equals(row.get("CostStatus")) && row.get(rAggregation) != null) {
                var amount = CostQueries.decimal(row.get(rAggregation));
                total = total.add(amount);
                if (currency != null) {
                    totalsByCurrency.merge(currency, amount, BigDecimal::add);
                }
            }
        }
        var mixedCurrencies = totalsByCurrency.size() > 1;

        return Output.builder()
            .forecast(forecast)
            .totalAmount(mixedCurrencies ? null : total)
            .totalAmountByCurrency(totalsByCurrency)
            .unit(totalsByCurrency.size() == 1 ? totalsByCurrency.keySet().iterator().next() : null)
            .build();
    }

    @Builder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Forecast by day",
            description = "One entry per day, keyed by the column names returned by Azure, e.g. `Cost`, `UsageDate`, `CostStatus` (`Forecast` or `Actual`), `Currency`."
        )
        private List<Map<String, Object>> forecast;

        @Schema(
            title = "Forecasted total",
            description = "Sum of the forecasted (non-`Actual`) entries of the aggregation column. Null when the entries are in more than one currency: use `totalAmountByCurrency` then."
        )
        private BigDecimal totalAmount;

        @Schema(
            title = "Forecasted total by currency",
            description = "Sum of the forecasted (non-`Actual`) entries per value of the `Currency` column, e.g. `totalAmountByCurrency.EUR`."
        )
        private Map<String, BigDecimal> totalAmountByCurrency;

        @Schema(
            title = "Currency",
            description = "Currency of the forecast, e.g. `USD` or `EUR`. Null when the entries are in more than one currency (possible at EA, MCA or CSP billing scopes)."
        )
        private String unit;
    }
}
