package io.kestra.plugin.azure.billing;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
                "notifications", Map.of(
                    "Actual_GreaterThan_80_Percent",
                    Map.of("enabled", true, "operator", "GreaterThan", "threshold", 80, "thresholdType", "Actual", "contactEmails", List.of("finops@example.com"))
                )
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
        var api = QueryTest.stubApi(spy);
        doReturn(Map.of("value", List.of(budget("monthly-total", 1000, 512.3)), "nextLink", "https://management.azure.com/budgets-page-2"))
            .doReturn(Map.of("value", List.of(budget("team-data", 200, 10))))
            .when(api).send(eq("GET"), anyString(), isNull());

        var output = spy.run(runContextFactory.of());

        assertThat(output.getSize(), is(2L));
        assertThat(output.getTruncated(), is(false));
        var first = output.getRows().getFirst();
        assertThat(first.get("name"), is("monthly-total"));
        assertThat(first.get("amount"), is(new BigDecimal("1000")));
        assertThat(first.get("currentSpendAmount"), is(new BigDecimal("512.3")));
        @SuppressWarnings("unchecked")
        var notification = ((List<Map<String, Object>>) first.get("notifications")).getFirst();
        assertThat(notification.get("name"), is("Actual_GreaterThan_80_Percent"));
        assertThat(notification.get("enabled"), is(true));
        assertThat(notification.get("operator"), is("GreaterThan"));
        assertThat(notification.get("threshold"), is(new BigDecimal("80")));
        assertThat(notification.get("contactEmails"), is(List.of("finops@example.com")));

        var urls = ArgumentCaptor.forClass(String.class);
        verify(api, times(2)).send(eq("GET"), urls.capture(), isNull());
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
        var api = QueryTest.stubApi(spy);
        doReturn(Map.of("value", List.of(budget("a", 1, 1)), "nextLink", "https://management.azure.com/more"))
            .when(api).send(eq("GET"), anyString(), isNull());

        var output = spy.run(runContextFactory.of());

        verify(api, times(1)).send(eq("GET"), anyString(), isNull());
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
        var api = QueryTest.stubApi(spy);
        doReturn(budget("monthly-total", 1000, 1200)).when(api).send(eq("GET"), anyString(), isNull());

        var output = spy.run(runContextFactory.of());

        assertThat(output.getBudget().get("amount"), is(new BigDecimal("1000")));
        assertThat((BigDecimal) output.getBudget().get("currentSpendAmount"), comparesEqualTo(new BigDecimal("1200")));
        assertThat(output.getBudget().get("forecastSpendUnit"), is("USD"));
        verify(api).send(
            eq("GET"),
            eq("https://management.azure.com" + SCOPE + "/providers/Microsoft.Consumption/budgets/monthly-total?api-version=2023-05-01"), isNull()
        );
    }

    @Test
    void givenNextLinkOutsideManagementHost_whenListBudgets_thenFails() throws Exception {
        var spy = spy(ListBudgets.builder().id("budgets").type(ListBudgets.class.getName()).scope(Property.ofValue(SCOPE)).build());
        var api = QueryTest.stubApi(spy);
        doReturn(Map.of("value", List.of(budget("a", 1, 1)), "nextLink", "https://attacker.example.com/budgets"))
            .when(api).send(eq("GET"), anyString(), isNull());

        assertThrows(IllegalStateException.class, () -> spy.run(runContextFactory.of()));
        verify(api, times(1)).send(eq("GET"), anyString(), isNull());
    }

    @Test
    void givenMalformedResponse_whenListBudgets_thenFailsWithClearMessage() throws Exception {
        var spy = spy(ListBudgets.builder().id("budgets").type(ListBudgets.class.getName()).scope(Property.ofValue(SCOPE)).build());
        var api = QueryTest.stubApi(spy);
        doReturn(Map.of("value", "not-a-list")).when(api).send(eq("GET"), anyString(), isNull());

        var e = assertThrows(IllegalStateException.class, () -> spy.run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("'value' should be an array"));
    }

    @Test
    void givenUnsafeBudgetName_whenGetBudget_thenFailsBeforeCallingAzure() throws Exception {
        for (var name : List.of("../other", "a/b", "a?api-version=1", "a#b", "a b", ".", "..", "...")) {
            var spy = spy(GetBudget.builder().id("budget").type(GetBudget.class.getName()).scope(Property.ofValue(SCOPE)).budgetName(Property.ofValue(name)).build());
            assertThrows(IllegalArgumentException.class, () -> spy.run(runContextFactory.of()), name);
            verify(spy, never()).api(any());
        }
    }

    @Test
    void givenNewBudgetWithoutCurrentSpend_whenGetBudget_thenSpendIsAbsent() throws Exception {
        var spy = spy(GetBudget.builder().id("budget").type(GetBudget.class.getName()).scope(Property.ofValue(SCOPE)).budgetName(Property.ofValue("new-budget")).build());
        var api = QueryTest.stubApi(spy);
        doReturn(Map.of("name", "new-budget", "properties", Map.of("category", "Cost", "amount", 500, "timeGrain", "Monthly")))
            .when(api).send(eq("GET"), anyString(), isNull());

        var budget = spy.run(runContextFactory.of()).getBudget();

        assertThat(budget.get("amount"), is(new BigDecimal("500")));
        assertThat(budget, not(hasKey("currentSpendAmount")));
        assertThat(budget.get("notifications"), is(List.of()));
    }

    @Test
    void givenTemplatedMaxPagesOfZero_whenListBudgets_thenFails() throws Exception {
        var spy = spy(
            ListBudgets.builder().id("budgets").type(ListBudgets.class.getName()).scope(Property.ofValue(SCOPE)).maxPages(Property.ofExpression("{{ 0 }}")).build()
        );

        assertThrows(IllegalArgumentException.class, () -> spy.run(runContextFactory.of()));
        verify(spy, never()).api(any());
    }
}
