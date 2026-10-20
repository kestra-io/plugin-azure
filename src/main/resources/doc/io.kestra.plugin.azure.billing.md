# How to use the Azure Billing plugin

The `billing` sub-plugin reads Azure Cost Management (cost and usage, forecasts) and Azure Consumption budgets from your Kestra flows, using the same Azure credentials as the rest of `plugin-azure`.

## Authentication and permissions

All billing tasks use the shared `plugin-azure` service principal authentication. The principal needs the **Cost Management Reader** role (or Reader) on the scope it queries.

```yaml
pluginDefaults:
  - type: io.kestra.plugin.azure
    values:
      tenantId: "{{ secret('AZURE_TENANT_ID') }}"
      clientId: "{{ secret('AZURE_CLIENT_ID') }}"
      clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
```

## Scope

Every task takes a `scope`, the Azure path the costs or budgets belong to:

| Scope | Value |
|---|---|
| Subscription | `/subscriptions/{subscriptionId}` |
| Resource group | `/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}` |
| Management group | `/providers/Microsoft.Management/managementGroups/{managementGroupId}` |
| Billing account (EA/MCA) | `/providers/Microsoft.Billing/billingAccounts/{billingAccountId}` |

## Choosing a task

| Task | Use when |
|---|---|
| `Query` | You want cost or usage for a period, optionally grouped by resource group, service, tag... |
| `Forecast` | You want the forecasted daily spend for an explicit period |
| `ListBudgets` | You want all budgets of a scope with their current and forecasted spend |
| `GetBudget` | You want one budget, for example to fail or alert when it is exceeded |

## Dates and timeframes

`Query` needs a `timeframe`. With `Custom`, set `startDate` and `endDate` as `yyyy-MM-dd`; both are **inclusive**, like in the Cost Management API (January is `2026-01-01` to `2026-01-31`). `Billing*` timeframes only make sense at EA/MCA billing scopes. `Forecast` always uses an explicit `startDate`/`endDate` and daily granularity, which is all the Forecast API supports.

## Rate limits

The Cost Management APIs are free, but they are throttled per scope and tenant. Request only the columns and groupings you need, schedule flows at a reasonable interval, and keep `maxPages` low (default 10): `Query` and `ListBudgets` stop there, log a warning and return `truncated: true`.

## Outputs

`Query` and `ListBudgets` follow the `fetchType` convention: `STORE` (default) writes an ION file and returns its `uri`, `FETCH` returns `rows`, `FETCH_ONE` returns `row`, and `NONE` returns only the counts. `size` is the number of rows in the output (1 or 0 for `FETCH_ONE`). `Query` also returns `costTotals`, the sum of each aggregation column (e.g. `costTotals.Cost`), `costTotalsByCurrency` (e.g. `costTotalsByCurrency.EUR.Cost`) and `unit`, the currency. `Forecast` returns the daily `forecast`, its `totalAmount`, `totalAmountByCurrency` and `unit`. EA, MCA and CSP billing scopes can return several currencies: then `costTotals`, `totalAmount` and `unit` are null, and the per-currency outputs hold the totals.
