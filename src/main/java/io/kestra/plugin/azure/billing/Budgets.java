package io.kestra.plugin.azure.billing;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Helpers shared by ListBudgets and GetBudget. */
final class Budgets {
    static final String API_VERSION = "2023-05-01";

    private Budgets() {
    }

    static String url(String scope, String budgetName) {
        return AbstractBilling.MANAGEMENT_ENDPOINT + scope + "/providers/Microsoft.Consumption/budgets" +
            (budgetName == null ? "" : "/" + budgetName) + "?api-version=" + API_VERSION;
    }

    static Map<String, Object> toMap(Map<String, Object> budget) {
        var properties = Objects.requireNonNullElse(object(budget.get("properties"), "properties"), Map.<String, Object> of());
        var map = new LinkedHashMap<String, Object>();
        map.put("name", budget.get("name"));
        map.put("id", budget.get("id"));
        map.put("category", properties.get("category"));
        map.put("amount", decimalOrNull(properties.get("amount")));
        map.put("timeGrain", properties.get("timeGrain"));
        var timePeriod = object(properties.get("timePeriod"), "properties.timePeriod");
        if (timePeriod != null) {
            map.put("startDate", timePeriod.get("startDate"));
            map.put("endDate", timePeriod.get("endDate"));
        }
        putSpend(map, "currentSpend", object(properties.get("currentSpend"), "properties.currentSpend"));
        putSpend(map, "forecastSpend", object(properties.get("forecastSpend"), "properties.forecastSpend"));
        var notifications = object(properties.get("notifications"), "properties.notifications");
        var notificationList = new ArrayList<Map<String, Object>>();
        if (notifications != null) {
            for (var entry : notifications.entrySet()) {
                var notification = Objects.requireNonNullElse(object(entry.getValue(), "properties.notifications." + entry.getKey()), Map.<String, Object> of());
                var item = new LinkedHashMap<String, Object>();
                item.put("name", entry.getKey());
                item.put("enabled", notification.get("enabled"));
                item.put("operator", notification.get("operator"));
                item.put("threshold", decimalOrNull(notification.get("threshold")));
                item.put("thresholdType", notification.get("thresholdType"));
                item.put("contactEmails", notification.getOrDefault("contactEmails", List.of()));
                item.put("contactRoles", notification.getOrDefault("contactRoles", List.of()));
                item.put("contactGroups", notification.getOrDefault("contactGroups", List.of()));
                notificationList.add(item);
            }
        }
        map.put("notifications", notificationList);
        return map;
    }

    /** Casts a JSON value to an object, failing with a clear message when Azure returned something else. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> object(Object value, String field) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Map<?, ?>)) {
            throw new IllegalStateException("Malformed Azure budget response: '" + field + "' should be an object, got " + value.getClass().getSimpleName());
        }
        return (Map<String, Object>) value;
    }

    /** Casts a JSON value to a list of objects, failing with a clear message when Azure returned something else. */
    static List<Map<String, Object>> list(Object value, String field) {
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> items)) {
            throw new IllegalStateException("Malformed Azure budget response: '" + field + "' should be an array, got " + value.getClass().getSimpleName());
        }
        var result = new ArrayList<Map<String, Object>>(items.size());
        for (var item : items) {
            result.add(object(item, field + "[]"));
        }
        return result;
    }

    private static void putSpend(Map<String, Object> map, String prefix, Map<String, Object> spend) {
        if (spend == null) {
            return;
        }
        map.put(prefix + "Amount", decimalOrNull(spend.get("amount")));
        map.put(prefix + "Unit", spend.get("unit"));
    }

    private static BigDecimal decimalOrNull(Object value) {
        return value == null ? null : new BigDecimal(value.toString());
    }
}
