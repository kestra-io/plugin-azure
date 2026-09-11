package io.kestra.plugin.azure.monitoring;

import java.util.Collections;

import com.azure.core.credential.TokenCredential;
import com.azure.monitor.ingestion.LogsIngestionClient;
import com.azure.monitor.ingestion.LogsIngestionClientBuilder;
import com.azure.monitor.query.metrics.MetricsClient;
import com.azure.monitor.query.metrics.MetricsClientBuilder;
import com.azure.monitor.query.metrics.MetricsServiceVersion;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.shared.AbstractAzureIdentityConnection;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractMonitoringTask extends AbstractAzureIdentityConnection {
    @Schema(
        title = "Azure Monitor regional endpoint",
        description = "For queries, the regional metrics endpoint, e.g. https://westeurope.metrics.monitor.azure.com. For ingestion, the Data Collection Endpoint, e.g. https://my-dce-a1b2.westeurope.ingest.monitor.azure.com"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> endpoint;

    protected MetricsClient queryClient(RunContext runContext) throws IllegalVariableEvaluationException {
        TokenCredential baseCredential = this.credentials(runContext);

        TokenCredential scopedCredential = requestContext ->
        {
            requestContext.setScopes(Collections.singletonList("https://metrics.monitor.azure.com/.default"));
            return baseCredential.getToken(requestContext);
        };

        return new MetricsClientBuilder()
            .credential(scopedCredential)
            .endpoint(runContext.render(endpoint).as(String.class).orElseThrow())
            .serviceVersion(MetricsServiceVersion.getLatest())
            .buildClient();
    }

    /** Overridable so tests can point the client at a stub transport, the SDK exposes no base URL seam of its own. */
    protected LogsIngestionClient ingestionClient(RunContext runContext) throws IllegalVariableEvaluationException {
        // the SDK sets the monitor.azure.com ingestion audience itself, so no scope wrapper is needed here
        return new LogsIngestionClientBuilder()
            .credential(this.credentials(runContext))
            .endpoint(runContext.render(endpoint).as(String.class).orElseThrow())
            .buildClient();
    }
}
