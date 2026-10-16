package io.kestra.plugin.azure.billing;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@KestraTest
class ForecastTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    @SuppressWarnings("unchecked")
    void givenForecast_whenRun_thenSumsForecastedDaysOnly() throws Exception {
        var spy = spy(
            Forecast.builder()
                .id("forecast")
                .type(Forecast.class.getName())
                .scope(Property.ofValue("/subscriptions/sub"))
                .costType(Property.ofValue(CostQueries.CostType.ActualCost))
                .startDate(Property.ofValue("2026-10-14"))
                .endDate(Property.ofValue("2026-10-16"))
                .aggregation(Property.ofValue("Cost"))
                .includeActualCost(Property.ofValue(true))
                .build()
        );
        var api = QueryTest.stubApi(spy);
        doReturn(
            Map.of(
                "properties", Map.of(
                    "columns", List.of(Map.of("name", "Cost"), Map.of("name", "UsageDate"), Map.of("name", "CostStatus"), Map.of("name", "Currency")),
                    "rows", List.of(
                        List.of(5, 20261014, "Actual", "USD"),
                        List.of(10.5, 20261015, "Forecast", "USD"),
                        List.of(11.25, 20261016, "Forecast", "USD")
                    )
                )
            )
        ).when(api).send(eq("POST"), anyString(), any());

        var output = spy.run(runContextFactory.of());

        assertThat(output.getForecast(), hasSize(3));
        assertThat(output.getTotalAmount(), comparesEqualTo(new BigDecimal("21.75")));
        assertThat(output.getUnit(), is("USD"));

        var url = ArgumentCaptor.forClass(String.class);
        var body = ArgumentCaptor.forClass(Map.class);
        verify(api).send(eq("POST"), url.capture(), body.capture());
        assertThat(url.getValue(), org.hamcrest.Matchers.endsWith("/subscriptions/sub/providers/Microsoft.CostManagement/forecast?api-version=2023-11-01"));
        Map<String, Object> sent = body.getValue();
        assertThat(sent.get("timeframe"), is("Custom"));
        assertThat(((Map<String, Object>) sent.get("dataset")).get("granularity"), is("Daily"));
        assertThat(sent.get("includeActualCost"), is(true));
    }
}
