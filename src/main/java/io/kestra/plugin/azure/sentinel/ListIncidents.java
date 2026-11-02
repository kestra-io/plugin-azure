package io.kestra.plugin.azure.sentinel;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List Sentinel incidents",
    description = """
        Lists and filters Sentinel incidents, following pagination and returning inline results or an ION file."""
)
@Plugin(examples = @Example(title = "List Sentinel incidents", full = true, code = """
    id: sentinel_listincidents
    namespace: company.security

    tasks:
      - id: sentinel
        type: io.kestra.plugin.azure.sentinel.ListIncidents
        tenantId: "{{ secret('AZURE_TENANT_ID') }}"
        clientId: "{{ secret('AZURE_CLIENT_ID') }}"
        clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
        subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
        resourceGroup: rg-security
        workspaceName: law-sentinel
        statuses: [New, Active]
        fetchType: STORE
    """))
public class ListIncidents extends AbstractSentinel implements RunnableTask<ListIncidents.Output> {
    @Schema(
        title = "OData filter",
        description = """
            Raw OData predicate, combined with severity and status filters using AND."""
    )
    @PluginProperty(group = "main")
    protected Property<String> filter;

    @Schema(
        title = "Incident severities",
        description = """
            Severities to keep: `High`, `Medium`, `Low` or `Informational`. Values within this list are combined using OR."""
    )
    @PluginProperty(group = "main")
    protected Property<List<IncidentSeverity>> severities;

    @Schema(
        title = "Incident statuses",
        description = """
            Statuses to keep: `New`, `Active` or `Closed`. Values within this list are combined using OR."""
    )
    @PluginProperty(group = "main")
    protected Property<List<IncidentStatus>> statuses;

    @Builder.Default
    @Schema(
        title = "Page size",
        description = """
            Number of incidents requested per page, between 1 and 1000. Defaults to 100.
            All pages are fetched unless `FETCH_ONE` is selected."""
    )
    @PluginProperty(group = "advanced")
    protected Property<@Min(1) @Max(1000) Integer> top = Property.ofValue(100);

    @Builder.Default
    @Schema(
        title = "Fetch type",
        description = """
            `STORE` (default) writes ION to internal storage; `FETCH` returns incidents; `FETCH_ONE` returns the first incident."""
    )
    @PluginProperty(group = "processing")
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public Output run(RunContext context) throws Exception {
        var rFetchType = context.render(fetchType).as(FetchType.class).orElse(FetchType.STORE);
        if (rFetchType != FetchType.FETCH && rFetchType != FetchType.FETCH_ONE && rFetchType != FetchType.STORE) {
            throw new IllegalArgumentException("fetchType must be FETCH, FETCH_ONE or STORE");
        }
        var rFilter = context.render(filter).as(String.class).orElse(null);
        var incidents = new ArrayList<Map<String, Object>>();
        var count = new long[] { 0 };
        if (rFetchType == FetchType.STORE) {
            var file = context.workingDir().createTempFile(".ion").toFile();
            try (var stream = new BufferedOutputStream(new FileOutputStream(file))) {
                incidentPages(context, rFilter, "properties/lastModifiedTimeUtc desc", page ->
                {
                    for (var incident : page) {
                        FileSerde.write(stream, incident);
                        count[0]++;
                    }
                    return true;
                });
            }
            return Output.builder().uri(context.storage().putFile(file)).count(count[0]).build();
        }
        incidentPages(context, rFilter, "properties/lastModifiedTimeUtc desc", page ->
        {
            if (rFetchType == FetchType.FETCH_ONE) {
                if (!page.isEmpty()) {
                    incidents.add(page.getFirst());
                    return false;
                }
            } else {
                incidents.addAll(page);
            }
            return true;
        });
        return rFetchType == FetchType.FETCH_ONE
            ? Output.builder().incident(incidents.isEmpty() ? null : incidents.getFirst()).count((long) incidents.size()).build()
            : Output.builder().incidents(incidents).count((long) incidents.size()).build();
    }

    void incidentPages(RunContext context, String rFilter, String order, PageConsumer consumer) throws Exception {
        var rTop = context.render(top).as(Integer.class).orElse(100);
        if (rTop < 1 || rTop > 1000) {
            throw new IllegalArgumentException("top must be between 1 and 1000");
        }
        var predicate = filters(rFilter, context.render(severities).asList(IncidentSeverity.class), context.render(statuses).asList(IncidentStatus.class));
        var query = new LinkedHashMap<String, String>();
        query.put("$orderby", order);
        query.put("$top", Integer.toString(rTop));
        if (!predicate.isBlank()) {
            query.put("$filter", predicate);
        }
        pages(context, incidentsUri(context, "", query), consumer);
    }

    static String filters(String raw, List<IncidentSeverity> severities, List<IncidentStatus> statuses) {
        var clauses = new ArrayList<String>();
        if (raw != null && !raw.isBlank()) {
            clauses.add("(" + raw + ")");
        }
        addFilter(clauses, "severity", severities);
        addFilter(clauses, "status", statuses);
        return String.join(" and ", clauses);
    }

    private static void addFilter(List<String> clauses, String field, List<? extends Enum<?>> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        clauses.add(
            "(" + values.stream().distinct().map(value -> "properties/" + field + " eq '" + value.name() + "'")
                .collect(Collectors.joining(" or ")) + ")"
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Fetched incidents",
            description = """
                Populated for FETCH."""
        )
        private final List<Map<String, Object>> incidents;
        @Schema(
            title = "First incident",
            description = """
                Populated for FETCH_ONE, null when no incident matches."""
        )
        private final Map<String, Object> incident;
        @Schema(
            title = "Stored incidents URI",
            description = """
                ION file in Kestra internal storage, populated for STORE."""
        )
        private final URI uri;
        @Schema(title = "Number of incidents fetched or stored")
        private final Long count;
    }

}
