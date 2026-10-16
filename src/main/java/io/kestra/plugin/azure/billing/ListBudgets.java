package io.kestra.plugin.azure.billing;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "List Azure Consumption budgets of a scope",
    description = "Calls the Consumption `Budgets - List` API and returns each budget with its amount, current spend and forecasted spend."
)
@Plugin(
    examples = {
        @Example(
            title = "List the budgets of a subscription and log how many there are.",
            full = true,
            code = """
                id: azure_list_budgets
                namespace: company.finops

                tasks:
                  - id: budgets
                    type: io.kestra.plugin.azure.billing.ListBudgets
                    scope: "/subscriptions/{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    fetchType: FETCH

                  - id: log_budgets
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.budgets.size }} budgets found"

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
public class ListBudgets extends AbstractBilling implements RunnableTask<ListBudgets.Output> {
    @Schema(
        title = "Maximum number of pages",
        description = "Stops after this many requests (following `nextLink`) and logs a warning if more budgets were available."
    )
    @Min(1)
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Integer> maxPages = Property.ofValue(DEFAULT_MAX_PAGES);

    @Schema(
        title = "Fetch strategy",
        description = "Default STORE writes all budgets to internal storage. FETCH returns all budgets, FETCH_ONE the first one, NONE only the count."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    @SuppressWarnings("unchecked")
    public Output run(RunContext runContext) throws Exception {
        int rMaxPages = runContext.render(maxPages).as(Integer.class).orElse(DEFAULT_MAX_PAGES);
        String url = Budgets.url(renderScope(runContext), null);
        var rows = new ArrayList<Map<String, Object>>();
        int pages = 0;

        try (var api = api(runContext)) {
            while (url != null && pages < rMaxPages) {
                var response = api.send("GET", url, null);
                pages++;
                for (var budget : (List<Map<String, Object>>) response.getOrDefault("value", List.of())) {
                    rows.add(Budgets.toMap(budget));
                }
                var nextLink = response.get("nextLink");
                url = nextLink == null || nextLink.toString().isBlank() ? null : requireManagementUrl(nextLink.toString());
            }
        }

        if (url != null) {
            runContext.logger().warn("Stopped after maxPages={} requests, more budgets are available. Increase maxPages to read them.", rMaxPages);
        }

        var fetched = BillingFetch.apply(runContext, runContext.render(fetchType).as(FetchType.class).orElse(FetchType.STORE), rows);
        return Output.builder()
            .rows(fetched.rows())
            .row(fetched.row())
            .uri(fetched.uri())
            .size(fetched.size())
            .truncated(url != null)
            .build();
    }

    @Builder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Budgets",
            description = "All budgets when fetchType is FETCH, each with `name`, `id`, `category`, `amount`, `timeGrain`, `startDate`/`endDate`, " +
                "`currentSpendAmount`/`currentSpendUnit`, `forecastSpendAmount`/`forecastSpendUnit` and the `notifications` names."
        )
        private List<Map<String, Object>> rows;

        @Schema(title = "First budget", description = "Single budget when fetchType is FETCH_ONE.")
        private Map<String, Object> row;

        @Schema(title = "Stored data URI", description = "Internal storage URI of an ION file with all budgets when fetchType is STORE.")
        private URI uri;

        @Schema(title = "Budget count", description = "Number of budgets returned.")
        private Long size;

        @Schema(title = "Truncated", description = "True when `maxPages` was reached before all budgets were read.")
        private Boolean truncated;
    }
}
