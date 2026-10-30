package io.kestra.plugin.azure.sentinel;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@KestraTest
class TriggerTest {
    private static final String TIME = "2026-01-01T00:00:00Z";
    private static final String LATER = "2026-01-01T00:01:00Z";

    @Inject
    RunContextFactory runContextFactory;

    private Trigger trigger(String id) {
        Trigger trigger = spy(
            Trigger.builder().id(id).type(Trigger.class.getName())
                .subscriptionId(Property.ofValue("subscription"))
                .resourceGroup(Property.ofValue("security"))
                .workspaceName(Property.ofValue("workspace"))
                .filter(Property.ofExpression("{{ 'properties/status eq false' }}"))
                .build()
        );
        doReturn(Instant.parse(TIME)).when(trigger).now();
        return trigger;
    }

    private void incidents(Trigger trigger, List<Map<String, Object>> rows) throws Exception {
        doAnswer(invocation ->
        {
            AbstractSentinel.PageConsumer consumer = invocation.getArgument(2);
            consumer.accept(rows);
            return null;
        }).when(trigger).pollIncidents(any(), any(), any());
    }

    private Map<String, Object> incident(String id, String time, String etag) {
        return Map.of("id", id, "etag", etag, "properties", Map.of("lastModifiedTimeUtc", time));
    }

    @Test
    void firstPollUsesLookbackAndRestartReadsBoundaryFromNamespaceStorage() throws Exception {
        Trigger first = trigger(UUID.randomUUID().toString());
        var context = TestsUtils.mockTrigger(runContextFactory, first);
        var runContext = context.getKey().getRunContext();
        var triggerContext = context.getValue().context();
        var rows = List.of(incident("first", TIME, "1"), incident("second", TIME, "1"));
        incidents(first, rows);
        var execution = first.evaluate(context.getKey(), triggerContext).orElseThrow();
        assertEquals(2, execution.getTrigger().getVariables().get("count"));
        assertEquals(rows, execution.getTrigger().getVariables().get("incidents"));
        verify(first).pollIncidents(any(), eq("(properties/status eq false) and (properties/lastModifiedTimeUtc ge 2025-12-31T00:00:00Z)"), any());

        String key = Trigger.stateKey(triggerContext.getFlowId(), first.getId());
        var stored = runContext.namespaceKv(triggerContext.getNamespace()).getValue(key).orElseThrow();
        var watermark = JacksonMapper.ofJson().readValue((byte[]) stored.value(), Trigger.Watermark.class);
        assertEquals(Instant.parse(TIME), watermark.timestamp());
        assertEquals(2, watermark.boundary().size());

        Trigger restarted = trigger(first.getId());
        incidents(restarted, rows);
        assertTrue(restarted.evaluate(context.getKey(), triggerContext).isEmpty());
        verify(restarted).pollIncidents(any(), eq("(properties/status eq false) and (properties/lastModifiedTimeUtc ge " + TIME + ")"), any());
    }

    @Test
    void fractionalVersionsSurvivePollsRestartsAndEtagChanges() throws Exception {
        String time = "2026-01-01T00:00:00.9876543Z";
        Trigger first = trigger(UUID.randomUUID().toString());
        var context = TestsUtils.mockTrigger(runContextFactory, first);
        var triggerContext = context.getValue().context();
        var rows = List.of(incident("first", time, "1"));
        incidents(first, rows);
        assertEquals(rows, first.evaluate(context.getKey(), triggerContext).orElseThrow().getTrigger().getVariables().get("incidents"));
        assertTrue(first.evaluate(context.getKey(), triggerContext).isEmpty());

        Trigger restarted = trigger(first.getId());
        incidents(restarted, rows);
        assertTrue(restarted.evaluate(context.getKey(), triggerContext).isEmpty());
        incidents(restarted, List.of(incident("first", "2026-01-01T00:00:00.9876549Z", "2")));
        assertTrue(restarted.evaluate(context.getKey(), triggerContext).isPresent());
        assertTrue(restarted.evaluate(context.getKey(), triggerContext).isEmpty());
        verify(restarted, atLeastOnce()).pollIncidents(any(), eq("(properties/status eq false) and (properties/lastModifiedTimeUtc ge 2026-01-01T00:00:00.987654Z)"), any());
    }

    @Test
    void fractionalBacklogDrainsAcrossPersistedPolls() throws Exception {
        String time = "2026-01-01T00:00:00.9876543Z";
        Trigger first = trigger(UUID.randomUUID().toString());
        first.maxIncidents = Property.ofValue(2);
        var context = TestsUtils.mockTrigger(runContextFactory, first);
        var triggerContext = context.getValue().context();
        var rows = List.of(incident("a", time, "1"), incident("b", time, "1"), incident("c", time, "1"));
        incidents(first, rows);
        assertEquals(rows.subList(0, 2), first.evaluate(context.getKey(), triggerContext).orElseThrow().getTrigger().getVariables().get("incidents"));
        Trigger restarted = trigger(first.getId());
        restarted.maxIncidents = Property.ofValue(2);
        incidents(restarted, rows);
        assertEquals(rows.subList(2, 3), restarted.evaluate(context.getKey(), triggerContext).orElseThrow().getTrigger().getVariables().get("incidents"));
        assertTrue(restarted.evaluate(context.getKey(), triggerContext).isEmpty());
    }

    @Test
    void readsExistingMicrosecondWatermarkWithoutReplayingItsBoundary() throws Exception {
        Trigger trigger = trigger(UUID.randomUUID().toString());
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var triggerContext = context.getValue().context();
        Instant storedTime = Instant.parse("2026-01-01T00:00:00.987654Z");
        var watermark = new Trigger.Watermark(storedTime, Set.of(new Trigger.Version("existing", storedTime, "1")));
        context.getKey().getRunContext().namespaceKv(triggerContext.getNamespace()).put(
            Trigger.stateKey(triggerContext.getFlowId(), trigger.getId()),
            new KVValueAndMetadata(new KVMetadata("existing watermark", (Duration) null), JacksonMapper.ofJson().writeValueAsBytes(watermark))
        );
        var unseen = incident("unseen", "2026-01-01T00:00:00.9876543Z", "1");
        incidents(trigger, List.of(incident("existing", "2026-01-01T00:00:00.9876543Z", "1"), unseen));
        assertEquals(List.of(unseen), trigger.evaluate(context.getKey(), triggerContext).orElseThrow().getTrigger().getVariables().get("incidents"));
        assertTrue(trigger.evaluate(context.getKey(), triggerContext).isEmpty());
    }

    @Test
    void fractionalInitialLookbackRetainsExactCutoff() throws Exception {
        Trigger trigger = trigger(UUID.randomUUID().toString());
        trigger.initialLookback = Property.ofValue(Duration.ofHours(1));
        doReturn(Instant.parse("2026-01-01T01:00:00.9876545Z")).when(trigger).now();
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var boundary = incident("boundary", "2026-01-01T00:00:00.9876545Z", "1");
        var after = incident("after", "2026-01-01T00:00:00.9876546Z", "1");
        incidents(trigger, List.of(incident("before", "2026-01-01T00:00:00.9876544Z", "1"), boundary, after));
        assertEquals(List.of(boundary, after), trigger.evaluate(context.getKey(), context.getValue().context()).orElseThrow().getTrigger().getVariables().get("incidents"));
        verify(trigger).pollIncidents(any(), eq("(properties/status eq false) and (properties/lastModifiedTimeUtc ge 2026-01-01T00:00:00.987654500Z)"), any());
    }

    @Test
    void rendersLookbackAndLimitAndExcludesOlderIncidents() throws Exception {
        Trigger trigger = trigger(UUID.randomUUID().toString());
        trigger.initialLookback = Property.ofExpression("{{ 'PT1H' }}");
        trigger.maxIncidents = Property.ofExpression("{{ 1 }}");
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        incidents(
            trigger, List.of(
                incident("old", "2025-12-31T22:59:59Z", "1"),
                incident("boundary", "2025-12-31T23:00:00Z", "1"),
                incident("later", TIME, "1")
            )
        );
        var execution = trigger.evaluate(context.getKey(), context.getValue().context()).orElseThrow();
        assertEquals(1, execution.getTrigger().getVariables().get("count"));
        assertEquals(List.of(incident("boundary", "2025-12-31T23:00:00Z", "1")), execution.getTrigger().getVariables().get("incidents"));
        verify(trigger).pollIncidents(any(), eq("(properties/status eq false) and (properties/lastModifiedTimeUtc ge 2025-12-31T23:00:00Z)"), any());
        // The initial window is no longer applied once a watermark exists.
        doReturn(Instant.parse(LATER).plus(Duration.ofDays(2))).when(trigger).now();
        assertEquals(1, trigger.evaluate(context.getKey(), context.getValue().context()).orElseThrow().getTrigger().getVariables().get("count"));
        verify(trigger, times(2)).pollIncidents(any(), eq("(properties/status eq false) and (properties/lastModifiedTimeUtc ge 2025-12-31T23:00:00Z)"), any());
    }

    @Test
    void rejectsInvalidBoundsBeforePolling() throws Exception {
        for (Duration invalid : List.of(Duration.ZERO, Duration.ofSeconds(-1))) {
            Trigger trigger = trigger(UUID.randomUUID().toString());
            trigger.initialLookback = Property.ofValue(invalid);
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            assertThrows(IllegalArgumentException.class, () -> trigger.evaluate(context.getKey(), context.getValue().context()));
            verify(trigger, never()).pollIncidents(any(), any(), any());
        }
        for (int invalid : List.of(0, -1)) {
            Trigger trigger = trigger(UUID.randomUUID().toString());
            trigger.maxIncidents = Property.ofValue(invalid);
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            assertThrows(IllegalArgumentException.class, () -> trigger.evaluate(context.getKey(), context.getValue().context()));
            verify(trigger, never()).pollIncidents(any(), any(), any());
        }
    }

    @Test
    void equalTimestampsAndChangedEtagsAreDeliveredOnceAndMaximumIsOrderIndependent() throws Exception {
        Trigger trigger = trigger(UUID.randomUUID().toString());
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        incidents(trigger, List.of(incident("first", TIME, "1")));
        trigger.evaluate(context.getKey(), context.getValue().context()).orElseThrow();
        // Equivalent to a complete multi-page result; the API helper separately tests pagination.
        incidents(trigger, List.of(incident("first", TIME, "2"), incident("second", TIME, "1"), incident("second", TIME, "1")));
        assertEquals(2, trigger.evaluate(context.getKey(), context.getValue().context()).orElseThrow().getTrigger().getVariables().get("count"));
        assertTrue(trigger.evaluate(context.getKey(), context.getValue().context()).isEmpty());

        incidents(trigger, List.of(incident("newer", LATER, "1"), incident("older", TIME, "1")));
        assertEquals(2, trigger.evaluate(context.getKey(), context.getValue().context()).orElseThrow().getTrigger().getVariables().get("count"));
        assertTrue(trigger.evaluate(context.getKey(), context.getValue().context()).isEmpty());
    }

    @Test
    void emptyAndFailedPollsDoNotAdvanceState() throws Exception {
        Trigger trigger = trigger(UUID.randomUUID().toString());
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var kv = context.getKey().getRunContext().namespaceKv(context.getValue().context().getNamespace());
        String key = Trigger.stateKey(context.getValue().context().getFlowId(), trigger.getId());
        incidents(trigger, List.of());
        assertTrue(trigger.evaluate(context.getKey(), context.getValue().context()).isEmpty());
        assertTrue(kv.getValue(key).isEmpty());
        incidents(trigger, List.of(incident("first", TIME, "1")));
        trigger.evaluate(context.getKey(), context.getValue().context());
        byte[] before = (byte[]) kv.getValue(key).orElseThrow().value();
        incidents(trigger, List.of());
        assertTrue(trigger.evaluate(context.getKey(), context.getValue().context()).isEmpty());
        assertArrayEquals(before, (byte[]) kv.getValue(key).orElseThrow().value());
        doThrow(new IllegalStateException("failed page")).when(trigger).pollIncidents(any(), any(), any());
        assertThrows(IllegalStateException.class, () -> trigger.evaluate(context.getKey(), context.getValue().context()));
        assertArrayEquals(before, (byte[]) kv.getValue(key).orElseThrow().value());
        incidents(trigger, List.of(incident("valid", LATER, "1"), Map.of("id", "invalid")));
        assertThrows(IllegalArgumentException.class, () -> trigger.evaluate(context.getKey(), context.getValue().context()));
        assertArrayEquals(before, (byte[]) kv.getValue(key).orElseThrow().value());
    }

    @Test
    void namespacesFlowsAndTriggersHaveIndependentState() throws Exception {
        Trigger trigger = trigger(UUID.randomUUID().toString());
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var original = context.getValue().context();
        incidents(trigger, List.of(incident("first", TIME, "1")));
        assertTrue(trigger.evaluate(context.getKey(), original).isPresent());
        assertTrue(trigger.evaluate(context.getKey(), original).isEmpty());
        var otherNamespaceContext = original.toBuilder().namespace(original.getNamespace() + ".other").build();
        var otherNamespaceFlow = ((Flow) context.getKey().getFlow()).toBuilder().namespace(otherNamespaceContext.getNamespace()).build();
        var otherNamespaceRunContext = runContextFactory.initializer().forScheduler(
            (DefaultRunContext) runContextFactory.of(otherNamespaceFlow, trigger), otherNamespaceContext, trigger
        );
        var otherNamespaceCondition = ConditionContext.builder().flow(otherNamespaceFlow).runContext(otherNamespaceRunContext).build();
        assertTrue(trigger.evaluate(otherNamespaceCondition, otherNamespaceContext).isPresent());
        assertTrue(trigger.evaluate(otherNamespaceCondition, otherNamespaceContext).isEmpty());
        assertTrue(trigger.evaluate(context.getKey(), original.toBuilder().flowId(original.getFlowId() + "_other").build()).isPresent());
        Trigger other = trigger(trigger.getId() + "_other");
        incidents(other, List.of(incident("first", TIME, "1")));
        assertTrue(other.evaluate(context.getKey(), original.toBuilder().triggerId(other.getId()).build()).isPresent());
        assertNotEquals(Trigger.stateKey("a_b", "c"), Trigger.stateKey("a", "b_c"));
    }

    @Test
    void executionFailureAndCorruptStateCannotConsumeIncidents() throws Exception {
        Trigger trigger = trigger(UUID.randomUUID().toString());
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var kv = context.getKey().getRunContext().namespaceKv(context.getValue().context().getNamespace());
        String key = Trigger.stateKey(context.getValue().context().getFlowId(), trigger.getId());
        incidents(trigger, List.of(incident("first", TIME, "1")));
        try (var service = mockStatic(TriggerService.class)) {
            service.when(() -> TriggerService.generateExecution(eq(trigger), any(), any(), any(Trigger.Output.class)))
                .thenThrow(new IllegalStateException("cannot construct execution"));
            assertThrows(IllegalStateException.class, () -> trigger.evaluate(context.getKey(), context.getValue().context()));
        }
        assertTrue(kv.getValue(key).isEmpty());
        kv.put(key, new KVValueAndMetadata(new KVMetadata("corrupt test state", (java.time.Duration) null), "invalid json".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        clearInvocations(trigger);
        assertThrows(Exception.class, () -> trigger.evaluate(context.getKey(), context.getValue().context()));
        verify(trigger, never()).pollIncidents(any(), any(), any());
    }
}
