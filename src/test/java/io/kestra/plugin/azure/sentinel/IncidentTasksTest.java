package io.kestra.plugin.azure.sentinel;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.*;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.github.tomakehurst.wiremock.WireMockServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;

import jakarta.inject.Inject;
import reactor.core.publisher.Mono;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@KestraTest
class IncidentTasksTest {
    private static final String PATH = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/law/providers/Microsoft.SecurityInsights/incidents";
    @Inject
    RunContextFactory factory;
    private WireMockServer server;
    private final AtomicReference<List<String>> scopes = new AtomicReference<>();

    @BeforeEach
    void start() {
        server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop();
    }

    private <T extends AbstractSentinel> T connect(T original) throws Exception {
        T task = spy(original);
        task.subscriptionId = Property.ofValue("sub");
        task.resourceGroup = Property.ofValue("rg");
        task.workspaceName = Property.ofValue("law");
        doReturn(URI.create(server.baseUrl())).when(task).armEndpoint();
        TokenCredential credential = request ->
        {
            scopes.set(request.getScopes());
            return Mono.just(new AccessToken("test-token", OffsetDateTime.now().plusHours(1)));
        };
        doReturn(credential).when(task).credentials(any());
        return task;
    }

    private ListIncidents listing(FetchType mode) throws Exception {
        return connect(ListIncidents.builder().id("list").type(ListIncidents.class.getName()).fetchType(Property.ofValue(mode)).build());
    }

    private void pages() {
        server.stubFor(get(urlPathEqualTo(PATH)).willReturn(okJson("{\"value\":[{\"name\":\"a\"}],\"nextLink\":\"" + server.baseUrl() + "/page2\"}")));
        server.stubFor(get(urlEqualTo("/page2")).willReturn(okJson("{\"value\":[{\"name\":\"b\"}]}")));
    }

    @Test
    void listsAllPagesAndRendersFiltersWithArmScope() throws Exception {
        pages();
        var task = listing(FetchType.FETCH);
        task.filter = Property.ofExpression("{{ inputs.filter }}");
        task.severities = Property.ofValue(List.of("High"));
        task.statuses = Property.ofValue(List.of("New", "Active"));
        task.top = Property.ofValue(200);
        var result = task.run(factory.of(Map.of("inputs", Map.of("filter", "properties/title eq 'A & B'"))));
        assertEquals(2L, result.getCount());
        assertEquals("b", result.getIncidents().getLast().get("name"));
        assertEquals(List.of(AbstractSentinel.ARM_SCOPE), scopes.get());
        server.verify(
            getRequestedFor(urlPathEqualTo(PATH)).withHeader("Authorization", equalTo("Bearer test-token"))
                .withQueryParam("api-version", equalTo("2024-09-01"))
                .withQueryParam("$top", equalTo("200"))
                .withQueryParam("$filter", equalTo("(properties/title eq 'A & B') and (properties/severity eq 'High') and (properties/status eq 'New' or properties/status eq 'Active')"))
        );
    }

    @Test
    void fetchOneStopsAfterFirstResultAndStoreRoundTripsIon() throws Exception {
        pages();
        var one = listing(FetchType.FETCH_ONE).run(factory.of());
        assertEquals(1L, one.getCount());
        assertEquals("a", one.getIncident().get("name"));
        server.verify(0, getRequestedFor(urlEqualTo("/page2")));
        RunContext context = factory.of();
        var stored = listing(FetchType.STORE).run(context);
        assertEquals(2L, stored.getCount());
        assertNull(stored.getIncidents());
        List<Object> records = new ArrayList<>();
        try (var input = context.storage().getFile(stored.getUri())) {
            FileSerde.read(input, records::add);
        }
        assertEquals(List.of(Map.of("name", "a"), Map.of("name", "b")), records);
    }

    @Test
    void emptyModesAndValidation() throws Exception {
        server.stubFor(get(urlPathEqualTo(PATH)).willReturn(okJson("{\"value\":[]}")));
        for (FetchType mode : List.of(FetchType.FETCH, FetchType.FETCH_ONE, FetchType.STORE)) {
            assertEquals(0L, listing(mode).run(factory.of()).getCount());
        }
        for (int invalid : List.of(0, 1001)) {
            var task = listing(FetchType.FETCH);
            task.top = Property.ofValue(invalid);
            assertThrows(IllegalArgumentException.class, () -> task.run(factory.of()));
        }
        assertThrows(IllegalArgumentException.class, () -> listing(FetchType.NONE).run(factory.of()));
        assertThrows(IllegalArgumentException.class, () -> ListIncidents.filters(null, List.of("Invalid"), List.of()));
    }

    @Test
    void rejectsCrossOriginRepeatedLinksAndMalformedPages() throws Exception {
        for (String link : List.of("https://example.com/stolen", server.baseUrl() + PATH + "?api-version=2024-09-01&%24orderby=properties%2FlastModifiedTimeUtc%20desc&%24top=100")) {
            server.stubFor(get(urlPathEqualTo(PATH)).willReturn(okJson("{\"value\":[],\"nextLink\":\"" + link + "\"}")));
            assertThrows(Exception.class, () -> listing(FetchType.FETCH).run(factory.of()));
        }
        server.stubFor(get(urlPathEqualTo(PATH)).willReturn(okJson("{\"value\":[1]}")));
        assertThrows(IllegalStateException.class, () -> listing(FetchType.FETCH).run(factory.of()));
    }

    @Test
    void includesAlertsOnlyWhenRequestedAndEncodesIncidentId() throws Exception {
        server.stubFor(get(urlPathEqualTo(PATH + "/incident one")).willReturn(okJson("{\"name\":\"incident one\"}")));
        // WireMock urlPath matching operates on the encoded path.
        server.stubFor(get(urlPathEqualTo(PATH + "/incident%20one")).willReturn(okJson("{\"name\":\"incident one\"}")));
        server.stubFor(post(urlPathEqualTo(PATH + "/incident%20one/alerts")).willReturn(okJson("{\"value\":[{\"kind\":\"SecurityAlert\"}]}")));
        var task = connect(GetIncident.builder().id("get").type(GetIncident.class.getName()).incidentId(Property.ofValue("incident one")).build());
        assertNull(task.run(factory.of()).alerts());
        task.includeAlerts = Property.ofValue(true);
        assertEquals(1, task.run(factory.of()).alerts().size());
        server.verify(1, postRequestedFor(urlPathEqualTo(PATH + "/incident%20one/alerts")));
    }

    @Test
    void commentsPaginateAndCreateUuidWithRenderedMessage() throws Exception {
        server.stubFor(get(urlPathEqualTo(PATH + "/i/comments")).willReturn(okJson("{\"value\":[{\"name\":\"c1\"}],\"nextLink\":\"/comments2\"}")));
        server.stubFor(get(urlEqualTo("/comments2")).willReturn(okJson("{\"value\":[{\"name\":\"c2\"}]}")));
        var list = connect(ListComments.builder().id("comments").type(ListComments.class.getName()).incidentId(Property.ofValue("i")).build());
        assertEquals(2L, list.run(factory.of()).count());
        server.stubFor(
            put(urlPathMatching(PATH + "/i/comments/[0-9a-f-]+"))
                .withRequestBody(equalToJson("{\"properties\":{\"message\":\"Investigating\"}}"))
                .willReturn(okJson("{\"properties\":{\"createdTimeUtc\":\"2026-10-04T00:00:00Z\"}}"))
        );
        var add = connect(
            AddComment.builder().id("add").type(AddComment.class.getName()).incidentId(Property.ofValue("i"))
                .message(Property.ofExpression("{{ inputs.message }}")).build()
        );
        var created = add.run(factory.of(Map.of("inputs", Map.of("message", "Investigating"))));
        assertEquals(created.commentId(), UUID.fromString(created.commentId()).toString());
        assertEquals("2026-10-04T00:00:00Z", created.createdTimeUtc());
    }

    private static Map<String, Object> existing() {
        return Map.of(
            "etag", "original", "id", "resource-id", "properties", Map.of(
                "title", "Attack", "severity", "High", "status", "New", "description", "Keep",
                "owner", Map.of("email", "old@example.com", "objectId", "owner-id"),
                "labels", List.of(Map.of("labelName", "system", "labelType", "System"), Map.of("labelName", "old", "labelType", "User")),
                "createdTimeUtc", "2026-10-04T00:00:00Z", "incidentNumber", 123
            )
        );
    }

    @Test
    void updatePreservesWritableFieldsAndEtagAndMergesOwnerAndLabels() throws Exception {
        server.stubFor(get(urlPathEqualTo(PATH + "/i")).willReturn(okJson(JacksonMapper.ofJson().writeValueAsString(existing()))));
        server.stubFor(put(urlPathEqualTo(PATH + "/i")).willReturn(okJson("{\"name\":\"i\"}")));
        var task = connect(
            UpdateIncident.builder().id("update").type(UpdateIncident.class.getName()).incidentId(Property.ofValue("i"))
                .status(Property.ofValue("Active")).ownerEmail(Property.ofValue("new@example.com")).labels(Property.ofValue(List.of("kestra"))).build()
        );
        assertEquals("i", task.run(factory.of()).incident().get("name"));
        var body = JacksonMapper.ofJson().readTree(server.findAll(putRequestedFor(urlPathEqualTo(PATH + "/i"))).getFirst().getBodyAsString());
        assertEquals("original", body.path("etag").asText());
        assertFalse(body.has("id"));
        var properties = body.path("properties");
        assertEquals("Attack", properties.path("title").asText());
        assertEquals("High", properties.path("severity").asText());
        assertEquals("Keep", properties.path("description").asText());
        assertEquals("Active", properties.path("status").asText());
        assertEquals("owner-id", properties.path("owner").path("objectId").asText());
        assertEquals("new@example.com", properties.path("owner").path("email").asText());
        assertFalse(properties.has("createdTimeUtc"));
        assertFalse(properties.has("incidentNumber"));
        assertEquals("system", properties.path("labels").get(0).path("labelName").asText());
        assertEquals("kestra", properties.path("labels").get(1).path("labelName").asText());
    }

    @Test
    void omittedAndEmptyLabelsDifferAndDescriptionIsNotTaskDescription() throws Exception {
        var omitted = UpdateIncident.merge(existing(), Map.of(), Map.of(), null);
        assertEquals(((Map<?, ?>) existing().get("properties")).get("labels"), ((Map<?, ?>) omitted.get("properties")).get("labels"));
        var cleared = (Map<?, ?>) UpdateIncident.merge(existing(), Map.of("description", "Changed"), Map.of(), List.of()).get("properties");
        assertEquals(List.of(Map.of("labelName", "system", "labelType", "System")), cleared.get("labels"));
        assertEquals("Changed", cleared.get("description"));
        assertThrows(IllegalStateException.class, () -> UpdateIncident.merge(Map.of("properties", Map.of()), Map.of(), Map.of(), null));
    }

    @Test
    void getFailurePreventsUpdateAndConflictIncludesAzureDetailsWithoutToken() throws Exception {
        var task = connect(UpdateIncident.builder().id("update").type(UpdateIncident.class.getName()).incidentId(Property.ofValue("i")).status(Property.ofValue("Active")).build());
        server.stubFor(get(urlPathEqualTo(PATH + "/i")).willReturn(aResponse().withStatus(403).withBody("{\"error\":{\"code\":\"AuthorizationFailed\",\"message\":\"Access denied\"}}")));
        Exception denied = assertThrows(IllegalStateException.class, () -> task.run(factory.of()));
        assertTrue(denied.getMessage().contains("403"));
        assertTrue(denied.getMessage().contains("AuthorizationFailed"));
        server.verify(0, putRequestedFor(urlPathEqualTo(PATH + "/i")));
        server.stubFor(get(urlPathEqualTo(PATH + "/i")).willReturn(okJson(JacksonMapper.ofJson().writeValueAsString(existing()))));
        server.stubFor(put(urlPathEqualTo(PATH + "/i")).willReturn(aResponse().withStatus(412).withBody("{\"error\":{\"code\":\"Conflict\",\"message\":\"test-token changed\"}}")));
        Exception conflict = assertThrows(IllegalStateException.class, () -> task.run(factory.of()));
        assertTrue(conflict.getMessage().contains("412"));
        assertFalse(conflict.getMessage().contains("test-token"));
        server.verify(1, putRequestedFor(urlPathEqualTo(PATH + "/i")));
    }

    @Test
    void updateRendersDescriptionAndClassificationWithoutChangingTaskDocumentation() throws Exception {
        server.stubFor(get(urlPathEqualTo(PATH + "/i")).willReturn(okJson(JacksonMapper.ofJson().writeValueAsString(existing()))));
        server.stubFor(put(urlPathEqualTo(PATH + "/i")).willReturn(okJson("{}")));
        var task = connect(
            UpdateIncident.builder().id("update").type(UpdateIncident.class.getName())
                .description("Kestra documentation").incidentId(Property.ofValue("i"))
                .incidentDescription(Property.ofExpression("{{ inputs.details }}"))
                .title(Property.ofValue("Investigated attack")).severity(Property.ofValue("Medium"))
                .status(Property.ofValue("Closed")).classification(Property.ofValue("TruePositive"))
                .classificationReason(Property.ofValue("SuspiciousActivity"))
                .classificationComment(Property.ofValue("Confirmed")).ownerObjectId(Property.ofValue("new-owner"))
                .build()
        );
        task.run(factory.of(Map.of("inputs", Map.of("details", "Investigation complete"))));
        var properties = JacksonMapper.ofJson().readTree(
            server.findAll(putRequestedFor(urlPathEqualTo(PATH + "/i")))
                .getFirst().getBodyAsString()
        ).path("properties");
        assertEquals("Investigation complete", properties.path("description").asText());
        assertEquals("Kestra documentation", task.getDescription());
        assertEquals("Investigated attack", properties.path("title").asText());
        assertEquals("Medium", properties.path("severity").asText());
        assertEquals("Closed", properties.path("status").asText());
        assertEquals("TruePositive", properties.path("classification").asText());
        assertEquals("SuspiciousActivity", properties.path("classificationReason").asText());
        assertEquals("Confirmed", properties.path("classificationComment").asText());
        assertEquals("new-owner", properties.path("owner").path("objectId").asText());
        assertEquals("old@example.com", properties.path("owner").path("email").asText());
    }

    @Test
    void failedLaterPageDoesNotReturnPartialSuccessAndRedirectsAreNotFollowed() throws Exception {
        pages();
        server.stubFor(get(urlEqualTo("/page2")).willReturn(aResponse().withStatus(500)));
        assertThrows(IllegalStateException.class, () -> listing(FetchType.STORE).run(factory.of()));
        server.stubFor(get(urlPathEqualTo(PATH)).willReturn(aResponse().withStatus(302).withHeader("Location", server.baseUrl() + "/redirected")));
        assertThrows(Exception.class, () -> listing(FetchType.FETCH).run(factory.of()));
        server.verify(0, getRequestedFor(urlEqualTo("/redirected")));
    }

}
