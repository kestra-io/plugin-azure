package io.kestra.plugin.azure.sentinel;

import java.time.Instant;
import java.util.List;
import java.util.Map;
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
        return spy(
            Trigger.builder().id(id).type(Trigger.class.getName())
                .subscriptionId(Property.ofValue("subscription"))
                .resourceGroup(Property.ofValue("security"))
                .workspaceName(Property.ofValue("workspace"))
                .filter(Property.ofExpression("{{ 'properties/status eq false' }}"))
                .build()
        );
    }

    private Map<String, Object> incident(String id, String time, String etag) {
        return Map.of("id", id, "etag", etag, "properties", Map.of("lastModifiedTimeUtc", time));
    }

    @Test
    void firstPollEmitsAllAndRestartReadsBoundaryFromNamespaceStorage() throws Exception {
        Trigger first = trigger(UUID.randomUUID().toString());
        var context = TestsUtils.mockTrigger(runContextFactory, first);
        var runContext = context.getKey().getRunContext();
        var triggerContext = context.getValue().context();
        var rows = List.of(incident("first", TIME, "1"), incident("second", TIME, "1"));
        doReturn(rows).when(first).pollIncidents(any(), any());
        var execution = first.evaluate(context.getKey(), triggerContext).orElseThrow();
        assertEquals(2, execution.getTrigger().getVariables().get("count"));
        assertEquals(rows, execution.getTrigger().getVariables().get("incidents"));
        verify(first).pollIncidents(any(), eq("properties/status eq false"));

        String key = Trigger.stateKey(triggerContext.getFlowId(), first.getId());
        var stored = runContext.namespaceKv(triggerContext.getNamespace()).getValue(key).orElseThrow();
        var watermark = JacksonMapper.ofJson().readValue((byte[]) stored.value(), Trigger.Watermark.class);
        assertEquals(Instant.parse(TIME), watermark.timestamp());
        assertEquals(2, watermark.boundary().size());

        Trigger restarted = trigger(first.getId());
        doReturn(rows).when(restarted).pollIncidents(any(), any());
        assertTrue(restarted.evaluate(context.getKey(), triggerContext).isEmpty());
        verify(restarted).pollIncidents(any(), eq("(properties/status eq false) and (properties/lastModifiedTimeUtc ge " + TIME + ")"));
    }

    @Test
    void equalTimestampsAndChangedEtagsAreDeliveredOnceAndMaximumIsOrderIndependent() throws Exception {
        Trigger trigger = trigger(UUID.randomUUID().toString());
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        doReturn(List.of(incident("first", TIME, "1"))).when(trigger).pollIncidents(any(), any());
        trigger.evaluate(context.getKey(), context.getValue().context()).orElseThrow();
        // Equivalent to a complete multi-page result; the API helper separately tests pagination.
        doReturn(List.of(incident("first", TIME, "2"), incident("second", TIME, "1"), incident("second", TIME, "1")))
            .when(trigger).pollIncidents(any(), any());
        assertEquals(2, trigger.evaluate(context.getKey(), context.getValue().context()).orElseThrow().getTrigger().getVariables().get("count"));
        assertTrue(trigger.evaluate(context.getKey(), context.getValue().context()).isEmpty());

        doReturn(List.of(incident("newer", LATER, "1"), incident("older", TIME, "1")))
            .when(trigger).pollIncidents(any(), any());
        assertEquals(2, trigger.evaluate(context.getKey(), context.getValue().context()).orElseThrow().getTrigger().getVariables().get("count"));
        assertTrue(trigger.evaluate(context.getKey(), context.getValue().context()).isEmpty());
    }

    @Test
    void emptyAndFailedPollsDoNotAdvanceState() throws Exception {
        Trigger trigger = trigger(UUID.randomUUID().toString());
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var kv = context.getKey().getRunContext().namespaceKv(context.getValue().context().getNamespace());
        String key = Trigger.stateKey(context.getValue().context().getFlowId(), trigger.getId());
        doReturn(List.of()).when(trigger).pollIncidents(any(), any());
        assertTrue(trigger.evaluate(context.getKey(), context.getValue().context()).isEmpty());
        assertTrue(kv.getValue(key).isEmpty());
        doReturn(List.of(incident("first", TIME, "1"))).when(trigger).pollIncidents(any(), any());
        trigger.evaluate(context.getKey(), context.getValue().context());
        byte[] before = (byte[]) kv.getValue(key).orElseThrow().value();
        doReturn(List.of()).when(trigger).pollIncidents(any(), any());
        assertTrue(trigger.evaluate(context.getKey(), context.getValue().context()).isEmpty());
        assertArrayEquals(before, (byte[]) kv.getValue(key).orElseThrow().value());
        doThrow(new IllegalStateException("failed page")).when(trigger).pollIncidents(any(), any());
        assertThrows(IllegalStateException.class, () -> trigger.evaluate(context.getKey(), context.getValue().context()));
        assertArrayEquals(before, (byte[]) kv.getValue(key).orElseThrow().value());
        doReturn(List.of(incident("valid", LATER, "1"), Map.of("id", "invalid"))).when(trigger).pollIncidents(any(), any());
        assertThrows(IllegalArgumentException.class, () -> trigger.evaluate(context.getKey(), context.getValue().context()));
        assertArrayEquals(before, (byte[]) kv.getValue(key).orElseThrow().value());
    }

    @Test
    void namespacesFlowsAndTriggersHaveIndependentState() throws Exception {
        Trigger trigger = trigger(UUID.randomUUID().toString());
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var original = context.getValue().context();
        doReturn(List.of(incident("first", TIME, "1"))).when(trigger).pollIncidents(any(), any());
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
        doReturn(List.of(incident("first", TIME, "1"))).when(other).pollIncidents(any(), any());
        assertTrue(other.evaluate(context.getKey(), original.toBuilder().triggerId(other.getId()).build()).isPresent());
        assertNotEquals(Trigger.stateKey("a_b", "c"), Trigger.stateKey("a", "b_c"));
    }

    @Test
    void executionFailureAndCorruptStateCannotConsumeIncidents() throws Exception {
        Trigger trigger = trigger(UUID.randomUUID().toString());
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var kv = context.getKey().getRunContext().namespaceKv(context.getValue().context().getNamespace());
        String key = Trigger.stateKey(context.getValue().context().getFlowId(), trigger.getId());
        doReturn(List.of(incident("first", TIME, "1"))).when(trigger).pollIncidents(any(), any());
        try (var service = mockStatic(TriggerService.class)) {
            service.when(() -> TriggerService.generateExecution(eq(trigger), any(), any(), any(Trigger.Output.class)))
                .thenThrow(new IllegalStateException("cannot construct execution"));
            assertThrows(IllegalStateException.class, () -> trigger.evaluate(context.getKey(), context.getValue().context()));
        }
        assertTrue(kv.getValue(key).isEmpty());
        kv.put(key, new KVValueAndMetadata(new KVMetadata("corrupt test state", (java.time.Duration) null), "invalid json".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        clearInvocations(trigger);
        assertThrows(Exception.class, () -> trigger.evaluate(context.getKey(), context.getValue().context()));
        verify(trigger, never()).pollIncidents(any(), any());
    }
}
