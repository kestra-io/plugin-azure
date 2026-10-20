package io.kestra.plugin.azure.billing;

import java.util.Map;
import java.util.regex.Pattern;

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
    title = "Get one Azure Consumption budget",
    description = "Calls the Consumption `Budgets - Get` API and returns the budget with its amount, current spend and forecasted spend."
)
@Plugin(
    examples = {
        @Example(
            title = "Fail the flow when a budget's current spend is above its amount.",
            full = true,
            code = """
                id: azure_check_budget
                namespace: company.finops

                tasks:
                  - id: budget
                    type: io.kestra.plugin.azure.billing.GetBudget
                    scope: "/subscriptions/{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    budgetName: monthly-total

                  - id: check
                    type: io.kestra.plugin.core.execution.Fail
                    condition: "{{ (outputs.budget.budget.currentSpendAmount ?? 0) > outputs.budget.budget.amount }}"
                    errorMessage: "Budget monthly-total exceeded: {{ outputs.budget.budget.currentSpendAmount }} {{ outputs.budget.budget.currentSpendUnit }}"

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
public class GetBudget extends AbstractBilling implements RunnableTask<GetBudget.Output> {
    private static final Pattern BUDGET_NAME = Pattern.compile("[A-Za-z0-9._-]+");

    @Schema(title = "Budget name", description = "Name of the budget to get. Letters, digits, `.`, `_` and `-` only.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> budgetName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rBudgetName = runContext.render(budgetName).as(String.class).orElseThrow(() -> new IllegalArgumentException("budgetName is required"));
        if (!BUDGET_NAME.matcher(rBudgetName).matches() || rBudgetName.chars().allMatch(c -> c == '.')) {
            throw new IllegalArgumentException("budgetName may only contain letters, digits, '.', '_' and '-' and cannot be only dots, got '" + rBudgetName + "'");
        }
        var url = Budgets.url(renderScope(runContext), rBudgetName);
        Map<String, Object> response;
        try (var api = api(runContext)) {
            response = api.send("GET", url, null);
        }
        return Output.builder().budget(Budgets.toMap(response)).build();
    }

    @Builder
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Budget",
            description = "The budget with `name`, `id`, `category`, `amount`, `timeGrain`, `startDate`/`endDate`, " +
                "`currentSpendAmount`/`currentSpendUnit`, `forecastSpendAmount`/`forecastSpendUnit`, and `notifications`, " +
                "each with `name`, `enabled`, `operator`, `threshold`, `thresholdType`, `contactEmails`, `contactRoles` and `contactGroups`."
        )
        private Map<String, Object> budget;
    }
}
