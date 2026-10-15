package io.kestra.plugin.azure.billing;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Helpers shared by ListBudgets and GetBudget to read Consumption budgets.
 */
final class Budgets {
    static final String API_VERSION = "2023-05-01";

    private Budgets() {
    }

    static String url(String scope, String budgetName) {
        return AbstractBilling.MANAGEMENT_ENDPOINT + scope + "/providers/Microsoft.Consumption/budgets" +
            (budgetName == null ? "" : "/" + budgetName) + "?api-version=" + API_VERSION;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> toMap(Map<String, Object> budget) {
        var properties = (Map<String, Object>) budget.getOrDefault("properties", Map.of());
        var map = new LinkedHashMap<String, Object>();
        map.put("name", budget.get("name"));
        map.put("id", budget.get("id"));
        map.put("category", properties.get("category"));
        map.put("amount", decimalOrNull(properties.get("amount")));
        map.put("timeGrain", properties.get("timeGrain"));
        var timePeriod = (Map<String, Object>) properties.get("timePeriod");
        if (timePeriod != null) {
            map.put("startDate", timePeriod.get("startDate"));
            map.put("endDate", timePeriod.get("endDate"));
        }
        putSpend(map, "currentSpend", (Map<String, Object>) properties.get("currentSpend"));
        putSpend(map, "forecastSpend", (Map<String, Object>) properties.get("forecastSpend"));
        var notifications = (Map<String, Object>) properties.get("notifications");
        map.put("notifications", notifications == null ? List.of() : List.copyOf(notifications.keySet()));
        return map;
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
