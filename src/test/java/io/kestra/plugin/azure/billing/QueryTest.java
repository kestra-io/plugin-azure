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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@KestraTest
class QueryTest {
    private static final String SCOPE = "/subscriptions/00000000-0000-0000-0000-000000000000";

    @Inject
    RunContextFactory runContextFactory;

    private Query.QueryBuilder<?, ?> task() {
        return Query.builder()
            .id("cost")
            .type(Query.class.getName())
            .tenantId(Property.ofValue("tenant"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .scope(Property.ofValue(SCOPE))
            .costType(Property.ofValue(CostQueries.CostType.ActualCost))
            .timeframe(Property.ofValue(Query.Timeframe.Custom))
            .startDate(Property.ofValue("2026-09-01"))
            .endDate(Property.ofValue("2026-09-30"))
            .granularity(Property.ofValue(Query.Granularity.None))
            .aggregations(Property.ofValue(List.of("Cost")));
    }

    static Map<String, Object> page(String nextLink, List<List<Object>> rows) {
        return Map.of(
            "properties", Map.of(
                "nextLink", nextLink == null ? "" : nextLink,
                "columns", List.of(
                    Map.of("name", "Cost", "type", "Number"),
                    Map.of("name", "ResourceGroupName", "type", "String"),
                    Map.of("name", "Currency", "type", "String")
                ),
                "rows", rows
            )
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void givenTwoPages_whenFetch_thenFollowsNextLinkAndSumsCost() throws Exception {
        var spy = spy(
            task()
                .groupBy(Property.ofValue(List.of("ResourceGroupName", "TAG:team")))
                .dimensionFilter(Property.ofValue(Map.of("ServiceName", List.of("Storage"))))
                .fetchType(Property.ofValue(FetchType.FETCH))
                .build()
        );
        doReturn(page("https://management.azure.com/next-page", List.of(List.of(12.5, "rg-data", "USD"))))
            .doReturn(page(null, List.of(List.of(0.75, "rg-web", "USD"))))
            .when(spy).send(any(RunContext.class), eq("POST"), anyString(), any());

        var output = spy.run(runContextFactory.of());

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows().get(0).get("ResourceGroupName"), is("rg-data"));
        assertThat(output.getCostTotals().get("Cost"), comparesEqualTo(new BigDecimal("13.25")));
        assertThat(output.getUnit(), is("USD"));
        assertThat(output.getTruncated(), is(false));

        var urls = ArgumentCaptor.forClass(String.class);
        var bodies = ArgumentCaptor.forClass(Map.class);
        verify(spy, times(2)).send(any(RunContext.class), eq("POST"), urls.capture(), bodies.capture());
        assertThat(urls.getAllValues().get(0), is("https://management.azure.com" + SCOPE + "/providers/Microsoft.CostManagement/query?api-version=2023-11-01"));
        assertThat(urls.getAllValues().get(1), is("https://management.azure.com/next-page"));

        Map<String, Object> body = bodies.getAllValues().getFirst();
        assertThat(body.get("type"), is("ActualCost"));
        assertThat(body.get("timeframe"), is("Custom"));
        assertThat(body.get("timePeriod"), is(Map.of("from", "2026-09-01T00:00:00Z", "to", "2026-09-30T23:59:59Z")));
        var dataset = (Map<String, Object>) body.get("dataset");
        assertThat(dataset.get("granularity"), is("None"));
        assertThat(dataset.get("aggregation"), is(Map.of("Cost", Map.of("name", "Cost", "function", "Sum"))));
        assertThat(
            dataset.get("grouping"), is(
                List.of(
                    Map.of("type", "Dimension", "name", "ResourceGroupName"),
                    Map.of("type", "TagKey", "name", "team")
                )
            )
        );
        assertThat(dataset.get("filter"), is(Map.of("dimensions", Map.of("name", "ServiceName", "operator", "In", "values", List.of("Storage")))));
    }

    @Test
    void givenMorePagesThanMaxPages_whenRun_thenStopsAndFlagsTruncated() throws Exception {
        var spy = spy(task().maxPages(Property.ofValue(2)).fetchType(Property.ofValue(FetchType.NONE)).build());
        doReturn(page("https://management.azure.com/more", List.of(List.of(1, "rg", "EUR"))))
            .when(spy).send(any(RunContext.class), eq("POST"), anyString(), any());

        var output = spy.run(runContextFactory.of());

        verify(spy, times(2)).send(any(RunContext.class), eq("POST"), anyString(), any());
        assertThat(output.getTruncated(), is(true));
        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows(), nullValue());
        assertThat(output.getUnit(), is("EUR"));
    }

    @Test
    void givenStore_whenRun_thenWritesFileToStorage() throws Exception {
        var spy = spy(task().timeframe(Property.ofValue(Query.Timeframe.MonthToDate)).startDate(null).endDate(null).build());
        doReturn(page(null, List.of(List.of(1, "a", "USD"), List.of(2, "b", "USD"))))
            .when(spy).send(any(RunContext.class), eq("POST"), anyString(), any());

        var output = spy.run(runContextFactory.of());

        assertThat(output.getUri(), notNullValue());
        assertThat(output.getSize(), is(2L));
    }

    @Test
    void givenInvalidInputs_whenRun_thenFailsBeforeCallingAzure() throws Exception {
        var cases = List.of(
            task().aggregations(Property.ofValue(List.of())).build(),
            task().groupBy(Property.ofValue(List.of("A", "B", "C"))).build(),
            task().startDate(null).build(),
            task().timeframe(Property.ofValue(Query.Timeframe.TheLastMonth)).build(),
            task().endDate(Property.ofValue("2026-08-01")).build(),
            task().startDate(Property.ofValue("09/01/2026")).build(),
            task().scope(Property.ofValue("subscriptions/abc")).build()
        );
        for (var task : cases) {
            var spy = spy(task);
            assertThrows(IllegalArgumentException.class, () -> spy.run(runContextFactory.of()));
            verify(spy, never()).send(any(), any(), any(), any());
        }
    }
}
