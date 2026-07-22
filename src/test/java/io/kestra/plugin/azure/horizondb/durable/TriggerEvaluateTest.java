package io.kestra.plugin.azure.horizondb.durable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Exercises {@link Trigger#evaluate} end to end — targetStatus rendering, dedup via
 * StatefulTriggerService, namespace KV state persistence, and Execution generation — without a
 * live HorizonDB instance, by overriding the single database-touching step
 * ({@link Trigger#pollInstances}) with a canned result. The dedup semantics themselves are
 * covered independently, at the unit level, by {@link TriggerComputeFiredTest}.
 */
@KestraTest
class TriggerEvaluateTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldFireOnceThenNotRefireForAnUnchangedInstance() throws Exception {
        ListInstances.Output polled = ListInstances.Output.builder()
            .instances(List.of(Map.of("instance_id", "i-1", "status", "Completed")))
            .size(1L)
            .build();

        TestTrigger trigger = new TestTrigger("trg-" + IdUtils.create(), Property.ofValue("Completed"), polled);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> first = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(first.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fired = (List<Map<String, Object>>) first.get().getTrigger().getVariables().get("instances");
        assertThat(fired, hasSize(1));

        // same instance, still Completed, polled a second time: must not refire
        Optional<Execution> second = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(second.isPresent(), is(false));
    }

    @Test
    void shouldNotFireWhenNoInstancesAreInTargetStatus() throws Exception {
        ListInstances.Output polled = ListInstances.Output.builder().instances(List.of()).size(0L).build();
        TestTrigger trigger = new TestTrigger("trg-" + IdUtils.create(), Property.ofValue("Completed"), polled);

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(false));
    }

    @Test
    void shouldFireAgainForANewlyAppearedInstanceOnASubsequentPoll() throws Exception {
        TestTrigger trigger = new TestTrigger(
            "trg-" + IdUtils.create(),
            Property.ofValue("Completed"),
            ListInstances.Output.builder()
                .instances(List.of(Map.of("instance_id", "i-1", "status", "Completed")))
                .size(1L)
                .build()
        );

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> first = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(first.isPresent(), is(true));

        // a different instance id reaches Completed on the next poll: must fire again
        trigger.setCanned(
            ListInstances.Output.builder()
                .instances(
                    List.of(
                        Map.of("instance_id", "i-1", "status", "Completed"),
                        Map.of("instance_id", "i-2", "status", "Completed")
                    )
                )
                .size(2L)
                .build()
        );

        Optional<Execution> second = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(second.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fired = (List<Map<String, Object>>) second.get().getTrigger().getVariables().get("instances");
        assertThat(fired, hasSize(1));
        assertThat(fired.get(0).get("instance_id"), is("i-2"));
    }

    /**
     * Skips the real JDBC connection normally opened by {@link Trigger#pollInstances} and
     * returns a canned {@link ListInstances.Output} instead.
     */
    private static class TestTrigger extends Trigger {
        private ListInstances.Output canned;

        TestTrigger(String id, Property<String> targetStatus, ListInstances.Output canned) {
            this.id = id;
            this.type = TestTrigger.class.getName();
            this.host = Property.ofValue("localhost");
            this.database = Property.ofValue("test");
            this.targetStatus = targetStatus;
            this.canned = canned;
        }

        void setCanned(ListInstances.Output canned) {
            this.canned = canned;
        }

        @Override
        protected ListInstances.Output pollInstances(RunContext runContext, String targetStatus) {
            return canned;
        }
    }
}
