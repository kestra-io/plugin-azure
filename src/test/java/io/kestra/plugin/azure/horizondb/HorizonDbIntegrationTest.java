package io.kestra.plugin.azure.horizondb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.azure.horizondb.durable.GetStatus;
import io.kestra.plugin.azure.horizondb.durable.Start;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

/**
 * Requires a live Azure HorizonDB instance (public preview) with pg_durable enabled.
 * Provide connection details via the referenced test properties to run these tests.
 */
@KestraTest
@Disabled("To run this test provide a live Azure HorizonDB instance with pg_durable enabled")
class HorizonDbIntegrationTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.variables.globals.azure.horizondb.host}")
    protected String host;

    @Value("${kestra.variables.globals.azure.horizondb.database}")
    protected String database;

    @Value("${kestra.variables.globals.azure.horizondb.username}")
    protected String username;

    @Value("${kestra.variables.globals.azure.horizondb.password}")
    protected String password;

    @Test
    void shouldQueryHorizonDb() throws Exception {
        RunContext runContext = runContextFactory.of();

        Query task = Query.builder()
            .id(HorizonDbIntegrationTest.class.getSimpleName())
            .type(Query.class.getName())
            .host(Property.ofValue(this.host))
            .database(Property.ofValue(this.database))
            .username(Property.ofValue(this.username))
            .password(Property.ofValue(this.password))
            .sql(Property.ofValue("SELECT 1 AS one"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        Query.Output output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getRow(), hasEntry("one", 1));
    }

    @Test
    void shouldStartAndTrackDurableInstance() throws Exception {
        RunContext runContext = runContextFactory.of();

        Start start = Start.builder()
            .id(HorizonDbIntegrationTest.class.getSimpleName())
            .type(Start.class.getName())
            .host(Property.ofValue(this.host))
            .database(Property.ofValue(this.database))
            .username(Property.ofValue(this.username))
            .password(Property.ofValue(this.password))
            .functionBody(Property.ofValue("'SELECT 1'"))
            .label(Property.ofValue("kestra-integration-test"))
            .build();

        Start.Output startOutput = start.run(runContext);
        assertThat(startOutput.getInstanceId(), notNullValue());

        GetStatus getStatus = GetStatus.builder()
            .id(HorizonDbIntegrationTest.class.getSimpleName())
            .type(GetStatus.class.getName())
            .host(Property.ofValue(this.host))
            .database(Property.ofValue(this.database))
            .username(Property.ofValue(this.username))
            .password(Property.ofValue(this.password))
            .instanceId(Property.ofValue(startOutput.getInstanceId()))
            .build();

        GetStatus.Output statusOutput = getStatus.run(runContext);
        assertThat(statusOutput.getStatus(), notNullValue());
    }
}
