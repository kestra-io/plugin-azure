package io.kestra.plugin.azure.horizondb.durable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.triggers.StatefulTriggerInterface.On;
import io.kestra.core.models.triggers.StatefulTriggerService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class TriggerComputeFiredTest {
    @Test
    void shouldFireForNewlySeenInstance() {
        Map<String, StatefulTriggerService.Entry> state = new HashMap<>();
        List<Map<String, Object>> instances = List.of(
            Map.of("instance_id", "i-1", "status", "Completed")
        );

        List<Map<String, Object>> fired = Trigger.computeFired(instances, state, On.CREATE_OR_UPDATE);

        assertThat(fired, hasSize(1));
        assertThat(state.keySet(), contains("i-1"));
    }

    @Test
    void shouldNotRefireForAnUnchangedInstanceOnASubsequentPoll() {
        Map<String, StatefulTriggerService.Entry> state = new HashMap<>();
        List<Map<String, Object>> instances = List.of(
            Map.of("instance_id", "i-1", "status", "Completed")
        );

        // first poll: fires and records the instance
        List<Map<String, Object>> firstPoll = Trigger.computeFired(instances, state, On.CREATE_OR_UPDATE);
        assertThat(firstPoll, hasSize(1));

        // second poll sees the same instance still in Completed status: must not refire
        List<Map<String, Object>> secondPoll = Trigger.computeFired(instances, state, On.CREATE_OR_UPDATE);
        assertThat(secondPoll, is(empty()));
    }

    @Test
    void shouldFireAgainWhenStatusChanges() {
        Map<String, StatefulTriggerService.Entry> state = new HashMap<>();

        Trigger.computeFired(List.of(Map.of("instance_id", "i-1", "status", "Running")), state, On.CREATE_OR_UPDATE);
        List<Map<String, Object>> fired = Trigger.computeFired(
            List.of(Map.of("instance_id", "i-1", "status", "Completed")),
            state,
            On.CREATE_OR_UPDATE
        );

        assertThat(fired, hasSize(1));
    }

    @Test
    void shouldSkipInstancesWithoutAnInstanceId() {
        Map<String, StatefulTriggerService.Entry> state = new HashMap<>();
        List<Map<String, Object>> instances = List.of(Map.of("status", "Completed"));

        List<Map<String, Object>> fired = Trigger.computeFired(instances, state, On.CREATE_OR_UPDATE);

        assertThat(fired, is(empty()));
    }

    @Test
    void shouldFireIndependentlyForMultipleInstances() {
        Map<String, StatefulTriggerService.Entry> state = new HashMap<>();
        List<Map<String, Object>> instances = List.of(
            Map.of("instance_id", "i-1", "status", "Completed"),
            Map.of("instance_id", "i-2", "status", "Completed")
        );

        List<Map<String, Object>> fired = Trigger.computeFired(instances, state, On.CREATE_OR_UPDATE);

        assertThat(fired, hasSize(2));
    }
}
