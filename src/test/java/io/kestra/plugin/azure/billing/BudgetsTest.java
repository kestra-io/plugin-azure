package io.kestra.plugin.azure.billing;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

@KestraTest
class BudgetsTest {
    private static final String SCOPE = "/subscriptions/sub";

    @Inject
    RunContextFactory runContextFactory;

    static Map<String, Object> budget(String name, int amount, double current) {
        return Map.of(
            "id", SCOPE + "/providers/Microsoft.Consumption/budgets/" + name,
            "name", name,
            "properties", Map.of(
                "category", "Cost",
                "amount", amount,
                "timeGrain", "Monthly",
                "timePeriod", Map.of("startDate", "2026-10-01T00:00:00Z"),
                "currentSpend", Map.of("amount", current, "unit", "USD"),
                "forecastSpend", Map.of("amount", 900, "unit", "USD"),
                "notifications", Map.of("Actual_GreaterThan_80_Percent", Map.of("enabled", true))
            )
        );
    }

    @Test
    void givenTwoPages_whenListBudgets_thenFollowsNextLink() throws Exception {
        var spy = spy(
            ListBudgets.builder()
                .id("budgets")
                .type(ListBudgets.class.getName())
                .scope(Property.ofValue(SCOPE))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .build()
        );
        doReturn(Map.of("value", List.of(budget("monthly-total", 1000, 512.3)), "nextLink", "https://management.azure.com/budgets-page-2"))
            .doReturn(Map.of("value", List.of(budget("team-data", 200, 10))))
            .when(spy).send(any(RunContext.class), eq("GET"), anyString(), isNull());

        var output = spy.run(runContextFactory.of());

        assertThat(output.getSize(), is(2L));
        assertThat(output.getTruncated(), is(false));
        var first = output.getRows().getFirst();
        assertThat(first.get("name"), is("monthly-total"));
        assertThat(first.get("amount"), is(new BigDecimal("1000")));
        assertThat(first.get("currentSpendAmount"), is(new BigDecimal("512.3")));
        assertThat(first.get("notifications"), is(List.of("Actual_GreaterThan_80_Percent")));

        var urls = ArgumentCaptor.forClass(String.class);
        verify(spy, times(2)).send(any(RunContext.class), eq("GET"), urls.capture(), isNull());
        assertThat(urls.getAllValues().get(0), is("https://management.azure.com" + SCOPE + "/providers/Microsoft.Consumption/budgets?api-version=2023-05-01"));
        assertThat(urls.getAllValues().get(1), is("https://management.azure.com/budgets-page-2"));
    }

    @Test
    void givenMaxPagesReached_whenListBudgets_thenTruncatedAndStored() throws Exception {
        var spy = spy(
            ListBudgets.builder()
                .id("budgets")
                .type(ListBudgets.class.getName())
                .scope(Property.ofValue(SCOPE))
                .maxPages(Property.ofValue(1))
                .build()
        );
        doReturn(Map.of("value", List.of(budget("a", 1, 1)), "nextLink", "https://management.azure.com/more"))
            .when(spy).send(any(RunContext.class), eq("GET"), anyString(), isNull());

        var output = spy.run(runContextFactory.of());

        verify(spy, times(1)).send(any(RunContext.class), eq("GET"), anyString(), isNull());
        assertThat(output.getTruncated(), is(true));
        assertThat(output.getUri(), notNullValue());
    }

    @Test
    void givenBudgetName_whenGetBudget_thenReturnsSpend() throws Exception {
        var spy = spy(
            GetBudget.builder()
                .id("budget")
                .type(GetBudget.class.getName())
                .scope(Property.ofValue(SCOPE + "/"))
                .budgetName(Property.ofValue("monthly-total"))
                .build()
        );
        doReturn(budget("monthly-total", 1000, 1200)).when(spy).send(any(RunContext.class), eq("GET"), anyString(), isNull());

        var output = spy.run(runContextFactory.of());

        assertThat(output.getBudget().get("amount"), is(new BigDecimal("1000")));
        assertThat((BigDecimal) output.getBudget().get("currentSpendAmount"), comparesEqualTo(new BigDecimal("1200")));
        assertThat(output.getBudget().get("forecastSpendUnit"), is("USD"));
        verify(spy).send(
            any(RunContext.class), eq("GET"),
            eq("https://management.azure.com" + SCOPE + "/providers/Microsoft.Consumption/budgets/monthly-total?api-version=2023-05-01"), isNull()
        );
    }
}
