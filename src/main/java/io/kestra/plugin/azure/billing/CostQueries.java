package io.kestra.plugin.azure.billing;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Helpers shared by Query and Forecast. */
final class CostQueries {
    static final String API_VERSION = "2023-11-01";

    private CostQueries() {
    }

    enum CostType {
        ActualCost,
        AmortizedCost,
        Usage
    }

    static Map<String, Object> timePeriod(LocalDate start, LocalDate end) {
        if (end.isBefore(start)) {
            throw new IllegalArgumentException("endDate (" + end + ") must not be before startDate (" + start + ")");
        }
        return Map.of("from", start + "T00:00:00Z", "to", end + "T23:59:59Z");
    }

    static Map<String, Object> aggregation(List<String> columns) {
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("At least one aggregation column is required, for example Cost or PreTaxCost");
        }
        var aggregation = new LinkedHashMap<String, Object>();
        for (var column : columns) {
            aggregation.put(column, Map.of("name", column, "function", "Sum"));
        }
        return aggregation;
    }

    /** {@code TAG:<key>} groups by a tag, anything else by a dimension such as {@code ResourceGroupName}. */
    static Map<String, Object> grouping(String value) {
        return value.startsWith("TAG:")
            ? Map.of("type", "TagKey", "name", value.substring(4))
            : Map.of("type", "Dimension", "name", value);
    }

    static Map<String, Object> filter(Map<String, ? extends List<?>> filters) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }
        var expressions = new ArrayList<Map<String, Object>>();
        for (var entry : filters.entrySet()) {
            var values = entry.getValue().stream().map(String::valueOf).toList();
            var key = entry.getKey();
            expressions.add(
                key.startsWith("TAG:")
                    ? Map.of("tags", Map.of("name", key.substring(4), "operator", "In", "values", values))
                    : Map.of("dimensions", Map.of("name", key, "operator", "In", "values", values))
            );
        }
        return expressions.size() == 1 ? expressions.getFirst() : Map.of("and", expressions);
    }

    /** Turns the {@code properties.columns} / {@code properties.rows} table of a response into one map per row. */
    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> rows(Map<String, Object> response) {
        var properties = (Map<String, Object>) response.getOrDefault("properties", Map.of());
        var columns = (List<Map<String, Object>>) properties.getOrDefault("columns", List.of());
        var rawRows = (List<List<Object>>) properties.getOrDefault("rows", List.of());
        var result = new ArrayList<Map<String, Object>>(rawRows.size());
        for (var rawRow : rawRows) {
            var row = new LinkedHashMap<String, Object>();
            for (int i = 0; i < columns.size() && i < rawRow.size(); i++) {
                row.put(String.valueOf(columns.get(i).get("name")), rawRow.get(i));
            }
            result.add(row);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    static String nextLink(Map<String, Object> response) {
        var properties = (Map<String, Object>) response.getOrDefault("properties", Map.of());
        var nextLink = properties.get("nextLink");
        return nextLink == null || nextLink.toString().isBlank() ? null : AbstractBilling.requireManagementUrl(nextLink.toString());
    }

    static BigDecimal decimal(Object value) {
        return value == null ? BigDecimal.ZERO : new BigDecimal(value.toString());
    }
}
