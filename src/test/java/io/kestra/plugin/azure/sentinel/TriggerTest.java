package io.kestra.plugin.azure.sentinel;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolationException;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class TriggerTest {
    private static final String TIME = "2026-01-01T00:00:00Z";
    private static final String LATER = "2026-01-01T00:01:00Z";
    private static final Instant NOW = Instant.parse(TIME);
    private static final String USER_FILTER = "properties/status eq false";
    private static final String LOOKBACK_FILTER = "(properties/status eq false) and (properties/lastModifiedTimeUtc ge 2025-12-31T00:00:00Z)";

    @Inject
    RunContextFactory runContextFactory;

    private final List<String> filters = new CopyOnWriteArrayList<>();

    private record State(KVStore kv, String key) {
    }

    private State state() throws Exception {
        var trigger = builder().build();
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var triggerContext = context.getValue().context();
        return new State(
            context.getKey().getRunContext().namespaceKv(triggerContext.getNamespace()),
            Trigger.stateKey(triggerContext.getFlowId(), trigger.getId())
        );
    }

    /** Mirrors Trigger#evaluate: load state, poll, persist only when something is delivered. */
    private List<Map<String, Object>> deliver(KVStore kv, String key, Instant now, Duration lookback, int limit, List<Map<String, Object>> rows) throws Exception {
        var poll = Trigger.poll(Trigger.load(kv, key), now, lookback, limit, USER_FILTER, (filter, consumer) ->
        {
            filters.add(filter);
            consumer.accept(rows);
        });
        if (!poll.fresh().isEmpty()) {
            Trigger.save(kv, key, poll.next());
        }
        return poll.fresh();
    }

    private List<Map<String, Object>> deliver(State state, List<Map<String, Object>> rows) throws Exception {
        return deliver(state.kv(), state.key(), NOW, Duration.ofDays(1), 100, rows);
    }

    private Map<String, Object> incident(String id, String time, String etag) {
        return Map.of("id", id, "etag", etag, "properties", Map.of("lastModifiedTimeUtc", time));
    }

    @Test
    void firstPollUsesLookbackAndNextPollReadsBoundaryFromNamespaceStorage() throws Exception {
        var state = state();
        var rows = List.of(incident("first", TIME, "1"), incident("second", TIME, "1"));
        assertEquals(rows, deliver(state, rows));
        assertEquals(LOOKBACK_FILTER, filters.getLast());

        var stored = state.kv().getValue(state.key()).orElseThrow();
        var watermark = JacksonMapper.ofJson().readValue((byte[]) stored.value(), Trigger.Watermark.class);
        assertEquals(NOW, watermark.timestamp());
        assertEquals(2, watermark.boundary().size());

        assertTrue(deliver(state, rows).isEmpty());
        assertEquals("(properties/status eq false) and (properties/lastModifiedTimeUtc ge " + TIME + ")", filters.getLast());
    }

    @Test
    void fractionalVersionsSurvivePollsAndEtagChanges() throws Exception {
        var state = state();
        var rows = List.of(incident("first", "2026-01-01T00:00:00.9876543Z", "1"));
        assertEquals(rows, deliver(state, rows));
        assertTrue(deliver(state, rows).isEmpty());

        var updated = List.of(incident("first", "2026-01-01T00:00:00.9876549Z", "2"));
        assertEquals(updated, deliver(state, updated));
        assertTrue(deliver(state, updated).isEmpty());
        assertTrue(filters.contains("(properties/status eq false) and (properties/lastModifiedTimeUtc ge 2026-01-01T00:00:00.987654Z)"));
    }

    @Test
    void fractionalBacklogDrainsAcrossPersistedPolls() throws Exception {
        var state = state();
        var time = "2026-01-01T00:00:00.9876543Z";
        var rows = List.of(incident("a", time, "1"), incident("b", time, "1"), incident("c", time, "1"));
        assertEquals(rows.subList(0, 2), deliver(state.kv(), state.key(), NOW, Duration.ofDays(1), 2, rows));
        assertEquals(rows.subList(2, 3), deliver(state.kv(), state.key(), NOW, Duration.ofDays(1), 2, rows));
        assertTrue(deliver(state.kv(), state.key(), NOW, Duration.ofDays(1), 2, rows).isEmpty());
    }

    @Test
    void readsExistingMicrosecondWatermarkWithoutReplayingItsBoundary() throws Exception {
        var state = state();
        var storedTime = Instant.parse("2026-01-01T00:00:00.987654Z");
        Trigger.save(state.kv(), state.key(), new Trigger.Watermark(storedTime, Set.of(new Trigger.Version("existing", storedTime, "1"))));
        var unseen = incident("unseen", "2026-01-01T00:00:00.9876543Z", "1");
        var rows = List.of(incident("existing", "2026-01-01T00:00:00.9876543Z", "1"), unseen);
        assertEquals(List.of(unseen), deliver(state, rows));
        assertTrue(deliver(state, rows).isEmpty());
    }

    @Test
    void fractionalInitialLookbackRetainsExactCutoff() throws Exception {
        var state = state();
        var boundary = incident("boundary", "2026-01-01T00:00:00.9876545Z", "1");
        var after = incident("after", "2026-01-01T00:00:00.9876546Z", "1");
        var rows = List.of(incident("before", "2026-01-01T00:00:00.9876544Z", "1"), boundary, after);
        var delivered = deliver(state.kv(), state.key(), Instant.parse("2026-01-01T01:00:00.9876545Z"), Duration.ofHours(1), 100, rows);
        assertEquals(List.of(boundary, after), delivered);
        assertEquals("(properties/status eq false) and (properties/lastModifiedTimeUtc ge 2026-01-01T00:00:00.987654500Z)", filters.getLast());
    }

    @Test
    void lookbackAndLimitExcludeOlderIncidentsAndAreIgnoredOnceAWatermarkExists() throws Exception {
        var state = state();
        var boundary = incident("boundary", "2025-12-31T23:00:00Z", "1");
        var rows = List.of(incident("old", "2025-12-31T22:59:59Z", "1"), boundary, incident("later", TIME, "1"));
        assertEquals(List.of(boundary), deliver(state.kv(), state.key(), NOW, Duration.ofHours(1), 1, rows));
        assertEquals("(properties/status eq false) and (properties/lastModifiedTimeUtc ge 2025-12-31T23:00:00Z)", filters.getLast());
        // The initial window is no longer applied once a watermark exists.
        var next = deliver(state.kv(), state.key(), NOW.plus(Duration.ofDays(2)), Duration.ofHours(1), 1, rows);
        assertEquals(List.of(incident("later", TIME, "1")), next);
        assertEquals("(properties/status eq false) and (properties/lastModifiedTimeUtc ge 2025-12-31T23:00:00Z)", filters.getLast());
    }

    private Trigger.TriggerBuilder<?, ?> builder() {
        return Trigger.builder()
            .id(IdUtils.create()).type(Trigger.class.getName())
            .subscriptionId(Property.ofValue("subscription"))
            .resourceGroup(Property.ofValue("security"))
            .workspaceName(Property.ofValue("workspace"));
    }

    @Test
    void rejectsInvalidBoundsBeforePolling() throws Exception {
        for (var invalid : List.of(Duration.ZERO, Duration.ofSeconds(-1))) {
            var trigger = builder().initialLookback(Property.ofValue(invalid)).build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            assertThrows(IllegalArgumentException.class, () -> trigger.evaluate(context.getKey(), context.getValue().context()));
        }
        for (var invalid : List.of(0, -1, 1001)) {
            var trigger = builder().maxIncidents(Property.ofValue(invalid)).build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            assertThrows(ConstraintViolationException.class, () -> trigger.evaluate(context.getKey(), context.getValue().context()));
        }
    }

    @Test
    void equalTimestampsAndChangedEtagsAreDeliveredOnceAndMaximumIsOrderIndependent() throws Exception {
        var state = state();
        assertEquals(1, deliver(state, List.of(incident("first", TIME, "1"))).size());
        // Equivalent to a complete multi-page result; the API helper separately tests pagination.
        assertEquals(2, deliver(state, List.of(incident("first", TIME, "2"), incident("second", TIME, "1"), incident("second", TIME, "1"))).size());
        assertTrue(deliver(state, List.of(incident("first", TIME, "2"), incident("second", TIME, "1"))).isEmpty());

        assertEquals(2, deliver(state, List.of(incident("newer", LATER, "1"), incident("older", TIME, "1"))).size());
        assertTrue(deliver(state, List.of(incident("newer", LATER, "1"), incident("older", TIME, "1"))).isEmpty());
    }

    @Test
    void emptyAndFailedPollsDoNotAdvanceState() throws Exception {
        var state = state();
        assertTrue(deliver(state, List.of()).isEmpty());
        assertTrue(state.kv().getValue(state.key()).isEmpty());
        deliver(state, List.of(incident("first", TIME, "1")));
        var before = (byte[]) state.kv().getValue(state.key()).orElseThrow().value();
        assertTrue(deliver(state, List.of()).isEmpty());
        assertArrayEquals(before, (byte[]) state.kv().getValue(state.key()).orElseThrow().value());
        assertThrows(IllegalStateException.class, () -> Trigger.poll(Trigger.load(state.kv(), state.key()), NOW, Duration.ofDays(1), 100, null, (filter, consumer) ->
        {
            throw new IllegalStateException("failed page");
        }));
        assertArrayEquals(before, (byte[]) state.kv().getValue(state.key()).orElseThrow().value());
        assertThrows(IllegalArgumentException.class, () -> deliver(state, List.of(incident("valid", LATER, "1"), Map.of("id", "invalid"))));
        assertArrayEquals(before, (byte[]) state.kv().getValue(state.key()).orElseThrow().value());
    }

    @Test
    void namespacesFlowsAndTriggersHaveIndependentState() throws Exception {
        var state = state();
        var rows = List.of(incident("first", TIME, "1"));
        assertEquals(1, deliver(state, rows).size());
        assertTrue(deliver(state, rows).isEmpty());
        var trigger = builder().build();
        var mocked = TestsUtils.mockTrigger(runContextFactory, trigger);
        var otherNamespaceContext = mocked.getValue().context().toBuilder().namespace(mocked.getValue().context().getNamespace() + ".other").build();
        var otherNamespaceFlow = ((Flow) mocked.getKey().getFlow()).toBuilder().namespace(otherNamespaceContext.getNamespace()).build();
        var otherNamespace = runContextFactory.initializer().forScheduler(
            (DefaultRunContext) runContextFactory.of(otherNamespaceFlow, trigger), otherNamespaceContext, trigger
        ).namespaceKv(otherNamespaceContext.getNamespace());
        assertEquals(1, deliver(otherNamespace, state.key(), NOW, Duration.ofDays(1), 100, rows).size());
        assertEquals(1, deliver(state.kv(), Trigger.stateKey("flow_" + IdUtils.create(), "trigger"), NOW, Duration.ofDays(1), 100, rows).size());
        assertEquals(1, deliver(state.kv(), Trigger.stateKey("flow", "trigger_" + IdUtils.create()), NOW, Duration.ofDays(1), 100, rows).size());
        assertNotEquals(Trigger.stateKey("a_b", "c"), Trigger.stateKey("a", "b_c"));
    }

    @Test
    void corruptOrIncompleteStateCannotConsumeIncidents() throws Exception {
        var state = state();
        state.kv().put(state.key(), new KVValueAndMetadata(new KVMetadata("corrupt test state", (Duration) null), "invalid json".getBytes(StandardCharsets.UTF_8)));
        assertThrows(Exception.class, () -> Trigger.load(state.kv(), state.key()));
        state.kv().put(state.key(), new KVValueAndMetadata(new KVMetadata("incomplete test state", (Duration) null), "{\"timestamp\":null}".getBytes(StandardCharsets.UTF_8)));
        assertThrows(IllegalStateException.class, () -> Trigger.load(state.kv(), state.key()));
    }
}
