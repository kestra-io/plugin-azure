package io.kestra.plugin.azure.horizondb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.postgresql.Driver;

/**
 * Shared connection handling for Azure HorizonDB tasks: opens a JDBC connection using either
 * password authentication or Azure Entra ID, hands it to the concrete task, then closes it.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractHorizonDb<T extends Output> extends Task {
    private static final String ENTRA_ID_AUTH_PLUGIN = "com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin";

    @Schema(
        title = "HorizonDB server host",
        description = "Hostname of the Azure HorizonDB server, without protocol or port (e.g. `myserver.postgres.horizondb.azure.com`)."
    )
    @NotNull
    @PluginProperty(group = "connection")
    private Property<String> host;

    @Schema(
        title = "HorizonDB server port",
        description = "Defaults to the standard PostgreSQL port."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<Integer> port = Property.ofValue(5432);

    @Schema(
        title = "Database name"
    )
    @NotNull
    @PluginProperty(group = "connection")
    private Property<String> database;

    @Schema(
        title = "Username",
        description = "Required unless useEntraId is true and the Entra ID token carries the identity."
    )
    @PluginProperty(group = "connection")
    private Property<String> username;

    @Schema(
        title = "Password",
        description = "Required unless useEntraId is true."
    )
    @PluginProperty(secret = true, group = "connection")
    private Property<String> password;

    @Schema(
        title = "Authenticate with Azure Entra ID",
        description = "When true, authenticates using Azure Entra ID (via the Azure Identity Extensions JDBC plugin) instead of a static password."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<Boolean> useEntraId = Property.ofValue(false);

    /**
     * Opens a JDBC connection to HorizonDB, delegates to the concrete task, then closes the connection.
     */
    public T run(RunContext runContext) throws Exception {
        registerDriver();

        String rHost = runContext.render(host).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("host is required"));
        Integer rPort = runContext.render(port).as(Integer.class).orElse(5432);
        String rDatabase = runContext.render(database).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("database is required"));
        boolean rUseEntraId = runContext.render(useEntraId).as(Boolean.class).orElse(false);

        String url = "jdbc:postgresql://" + rHost + ":" + rPort + "/" + rDatabase;

        Properties props = new Properties();
        runContext.render(username).as(String.class).ifPresent(u -> props.setProperty("user", u));

        if (rUseEntraId) {
            props.setProperty("authenticationPluginClassName", ENTRA_ID_AUTH_PLUGIN);
        } else {
            runContext.render(password).as(String.class).ifPresent(p -> props.setProperty("password", p));
        }

        try (Connection connection = DriverManager.getConnection(url, props)) {
            return run(runContext, connection);
        }
    }

    protected abstract T run(RunContext runContext, Connection connection) throws Exception;

    private void registerDriver() throws SQLException {
        // only register the driver if not already registered, to avoid a memory leak on repeated task runs
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }

    /**
     * Binds a nullable value onto a prepared statement parameter, falling back to a typed NULL
     * when the value is absent so drivers that reject untyped nulls (setObject(idx, null)) still work.
     */
    protected static void bind(PreparedStatement statement, int index, Object value) throws SQLException {
        if (value == null) {
            statement.setNull(index, java.sql.Types.VARCHAR);
        } else {
            statement.setObject(index, value);
        }
    }

    /**
     * Converts the current row of a ResultSet into an ordered column-name -&gt; value map.
     */
    protected static Map<String, Object> mapRow(ResultSet rs, ResultSetMetaData metaData) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            row.put(metaData.getColumnLabel(i), rs.getObject(i));
        }
        return row;
    }

    protected static Statement createStatement(Connection connection) throws SQLException {
        return connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }
}
