package io.kestra.plugin.azure.sentinel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

/** Read-only live smoke tests against an explicitly configured disposable Sentinel workspace. */
@KestraTest
@EnabledIfEnvironmentVariable(named = "AZURE_SENTINEL_LIVE_TEST", matches = "true")
class SentinelLiveTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void listIncidents() throws Exception {
        var task = ListIncidents.builder()
            .id("sentinel_live_list")
            .type(ListIncidents.class.getName())
            .tenantId(required("AZURE_TENANT_ID"))
            .clientId(required("AZURE_CLIENT_ID"))
            .clientSecret(required("AZURE_CLIENT_SECRET"))
            .subscriptionId(required("AZURE_SENTINEL_SUBSCRIPTION_ID"))
            .resourceGroup(required("AZURE_SENTINEL_RESOURCE_GROUP"))
            .workspaceName(required("AZURE_SENTINEL_WORKSPACE_NAME"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        var output = task.run(runContextFactory.of());
        assertEquals(1L, output.getCount());
        assertNotNull(output.getIncident());
    }

    @Test
    void readIncidentAndComments() throws Exception {
        var task = GetIncident.builder()
            .id("sentinel_live_get")
            .type(GetIncident.class.getName())
            .tenantId(required("AZURE_TENANT_ID"))
            .clientId(required("AZURE_CLIENT_ID"))
            .clientSecret(required("AZURE_CLIENT_SECRET"))
            .subscriptionId(required("AZURE_SENTINEL_SUBSCRIPTION_ID"))
            .resourceGroup(required("AZURE_SENTINEL_RESOURCE_GROUP"))
            .workspaceName(required("AZURE_SENTINEL_WORKSPACE_NAME"))
            .incidentId(required("AZURE_SENTINEL_INCIDENT_ID"))
            .includeAlerts(Property.ofValue(true))
            .build();

        var output = task.run(runContextFactory.of());
        assertEquals(System.getenv("AZURE_SENTINEL_INCIDENT_ID"), output.incident().get("name"));
        assertNotNull(output.alerts());

        var comments = ListComments.builder()
            .id("sentinel_live_comments")
            .type(ListComments.class.getName())
            .tenantId(required("AZURE_TENANT_ID"))
            .clientId(required("AZURE_CLIENT_ID"))
            .clientSecret(required("AZURE_CLIENT_SECRET"))
            .subscriptionId(required("AZURE_SENTINEL_SUBSCRIPTION_ID"))
            .resourceGroup(required("AZURE_SENTINEL_RESOURCE_GROUP"))
            .workspaceName(required("AZURE_SENTINEL_WORKSPACE_NAME"))
            .incidentId(required("AZURE_SENTINEL_INCIDENT_ID"))
            .build()
            .run(runContextFactory.of());
        assertNotNull(comments.comments());
        assertEquals((long) comments.comments().size(), comments.count());
    }

    @Test
    void queryWorkspace() throws Exception {
        var output = Query.builder()
            .id("sentinel_live_query")
            .type(Query.class.getName())
            .tenantId(required("AZURE_TENANT_ID"))
            .clientId(required("AZURE_CLIENT_ID"))
            .clientSecret(required("AZURE_CLIENT_SECRET"))
            .workspaceId(required("AZURE_SENTINEL_WORKSPACE_ID"))
            .query(Property.ofValue("print sentinel_live_test = 'ok'"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build()
            .run(runContextFactory.of());
        assertEquals(1L, output.getCount());
        assertEquals("ok", output.getRow().get("sentinel_live_test"));
    }

    private static Property<String> required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Opted-in Sentinel live tests require " + name);
        }
        return Property.ofValue(value);
    }
}
