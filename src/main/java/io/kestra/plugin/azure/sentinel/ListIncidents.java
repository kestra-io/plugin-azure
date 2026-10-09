package io.kestra.plugin.azure.sentinel;

import java.io.*;
import java.net.URI;
import java.util.*;

import io.kestra.core.models.annotations.*;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "List Sentinel incidents", description = "Lists and filters Sentinel incidents, following pagination and returning inline results or an ION file.")
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
    @Schema(title = "OData filter", description = "Raw OData predicate, combined with severity and status filters using AND.")
    protected Property<String> filter;

    @Schema(title = "Incident severities", description = "High, Medium, Low or Informational; values within this list are combined using OR.")
    protected Property<List<String>> severities;

    @Schema(title = "Incident statuses", description = "New, Active or Closed; values within this list are combined using OR.")
    protected Property<List<String>> statuses;

    @Builder.Default
    @Schema(title = "Page size", description = "Between 1 and 1000, defaults to 100. All pages are fetched unless FETCH_ONE is selected.")
    protected Property<Integer> top = Property.ofValue(100);

    @Builder.Default
    @Schema(title = "Fetch type", description = "STORE (default) writes ION to internal storage; FETCH returns incidents; FETCH_ONE returns the first incident.")
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public Output run(RunContext context) throws Exception {
        int pageSize = context.render(top).as(Integer.class).orElse(100);
        if (pageSize < 1 || pageSize > 1000) {
            throw new IllegalArgumentException("top must be between 1 and 1000");
        }
        FetchType mode = context.render(fetchType).as(FetchType.class).orElse(FetchType.STORE);
        if (mode != FetchType.FETCH && mode != FetchType.FETCH_ONE && mode != FetchType.STORE) {
            throw new IllegalArgumentException("fetchType must be FETCH, FETCH_ONE or STORE");
        }
        String predicate = filters(
            context.render(filter).as(String.class).orElse(null),
            context.render(severities).asList(String.class), context.render(statuses).asList(String.class)
        );
        Map<String, String> query = new LinkedHashMap<>();
        query.put("$orderby", "properties/lastModifiedTimeUtc desc");
        query.put("$top", Integer.toString(pageSize));
        if (!predicate.isBlank()) {
            query.put("$filter", predicate);
        }
        URI uri = incidentsUri(context, "", query);
        List<Map<String, Object>> incidents = new ArrayList<>();
        long[] count = { 0 };
        if (mode == FetchType.STORE) {
            File file = context.workingDir().createTempFile(".ion").toFile();
            try (var stream = new BufferedOutputStream(new FileOutputStream(file))) {
                pages(context, uri, page ->
                {
                    for (Map<String, Object> incident : page) {
                        FileSerde.write(stream, incident);
                        count[0]++;
                    }
                    return true;
                });
            }
            return Output.builder().uri(context.storage().putFile(file)).count(count[0]).build();
        }
        pages(context, uri, page ->
        {
            if (mode == FetchType.FETCH_ONE) {
                if (!page.isEmpty()) {
                    incidents.add(page.getFirst());
                    return false;
                }
            } else {
                incidents.addAll(page);
            }
            return true;
        });
        return mode == FetchType.FETCH_ONE
            ? Output.builder().incident(incidents.isEmpty() ? null : incidents.getFirst()).count((long) incidents.size()).build()
            : Output.builder().incidents(incidents).count((long) incidents.size()).build();
    }

    static String filters(String raw, List<String> severities, List<String> statuses) {
        List<String> clauses = new ArrayList<>();
        if (raw != null && !raw.isBlank()) {
            clauses.add("(" + raw + ")");
        }
        addFilter(clauses, "severity", severities, Set.of("High", "Medium", "Low", "Informational"));
        addFilter(clauses, "status", statuses, Set.of("New", "Active", "Closed"));
        return String.join(" and ", clauses);
    }

    private static void addFilter(List<String> clauses, String field, List<String> values, Set<String> allowed) {
        if (values == null || values.isEmpty()) {
            return;
        }
        if (values.stream().anyMatch(value -> value == null || !allowed.contains(value))) {
            throw new IllegalArgumentException("Invalid Sentinel " + field + "; expected one of " + allowed);
        }
        clauses.add(
            "(" + values.stream().distinct().map(value -> "properties/" + field + " eq '" + value + "'")
                .collect(java.util.stream.Collectors.joining(" or ")) + ")"
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Fetched incidents", description = "Populated for FETCH.")
        private final List<Map<String, Object>> incidents;
        @Schema(title = "First incident", description = "Populated for FETCH_ONE, null when no incident matches.")
        private final Map<String, Object> incident;
        @Schema(title = "Stored incidents URI", description = "ION file in Kestra internal storage, populated for STORE.")
        private final URI uri;
        @Schema(title = "Number of incidents fetched or stored")
        private final Long count;
    }

}
