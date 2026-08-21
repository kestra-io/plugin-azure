package io.kestra.plugin.azure.horizondb;

import java.net.URI;
import java.net.URISyntaxException;
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
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared connection handling for Azure HorizonDB tasks: opens a JDBC connection using either
 * password authentication or Azure Entra ID, hands it to the concrete task, then closes it.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractHorizonDb<T extends Output> extends Task implements RunnableTask<T> {
    private static final Logger log = LoggerFactory.getLogger(AbstractHorizonDb.class);

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
    @ToString.Exclude
    private Property<String> password;

    @Schema(
        title = "Authenticate with Azure Entra ID",
        description = "When true, authenticates using Azure Entra ID (via the Azure Identity Extensions JDBC plugin) instead of a static password. With no further properties set, this falls back to whatever DefaultAzureCredential resolves on the worker (managed identity, environment variables, Azure CLI login, etc.); set tenantId/clientId/clientSecret below to authenticate as a specific service principal instead."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<Boolean> useEntraId = Property.ofValue(false);

    @Schema(
        title = "Azure tenant id",
        description = "Used with clientId/clientSecret for service principal authentication when useEntraId is true. Ignored otherwise."
    )
    @PluginProperty(group = "connection")
    private Property<String> tenantId;

    @Schema(
        title = "Azure client id",
        description = "Used with tenantId/clientSecret for service principal authentication when useEntraId is true. Ignored otherwise."
    )
    @PluginProperty(group = "connection")
    private Property<String> clientId;

    @Schema(
        title = "Azure client secret",
        description = "Used with tenantId/clientId for service principal authentication when useEntraId is true. Ignored otherwise."
    )
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> clientSecret;

    @Schema(
        title = "Require TLS",
        description = "When true (the default), the connection is rejected unless it is encrypted (`sslmode=require`). Set to false only for local development against a non-TLS instance."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<Boolean> ssl = Property.ofValue(true);

    // Tracked so kill() (invoked from a different thread when the execution is killed or times
    // out) can cancel the in-flight statement and close the connection server-side, instead of
    // leaving the query running on HorizonDB after the Kestra execution has stopped. Mirrors the
    // pattern used by plugin-jdbc's AbstractJdbcQuery. Never part of equals/toString/the builder.
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private transient volatile Statement runningStatement;

    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private transient volatile Connection runningConnection;

    /**
     * Opens a JDBC connection to HorizonDB, delegates to the concrete task, then closes the connection.
     */
    @Override
    public T run(RunContext runContext) throws Exception {
        String rHost = runContext.render(host).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("host is required"));
        Integer rPort = runContext.render(port).as(Integer.class).orElse(5432);
        String rDatabase = runContext.render(database).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("database is required"));
        boolean rUseEntraId = runContext.render(useEntraId).as(Boolean.class).orElse(false);
        boolean rSsl = runContext.render(ssl).as(Boolean.class).orElse(true);

        String url;
        try {
            url = buildJdbcUrl(rHost, rPort, rDatabase);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("host or database contains characters that cannot form a valid connection URL: " + e.getMessage(), e);
        }

        Properties props = buildConnectionProperties(
            rSsl,
            runContext.render(username).as(String.class).orElse(null),
            rUseEntraId,
            runContext.render(password).as(String.class).orElse(null),
            runContext.render(tenantId).as(String.class).orElse(null),
            runContext.render(clientId).as(String.class).orElse(null),
            runContext.render(clientSecret).as(String.class).orElse(null)
        );

        // The org.postgresql driver self-registers with java.sql.DriverManager via the standard
        // JDBC 4 ServiceLoader mechanism (META-INF/services/java.sql.Driver); no manual
        // DriverManager.registerDriver call is needed.
        try (Connection connection = DriverManager.getConnection(url, props)) {
            this.runningConnection = connection;
            return run(runContext, connection);
        } finally {
            this.runningConnection = null;
        }
    }

    /**
     * Builds the JDBC connection {@link Properties} from already-rendered values. Extracted as a
     * pure function (no RunContext, no I/O) so the branching between password and Entra ID /
     * service-principal authentication can be unit tested directly.
     */
    static Properties buildConnectionProperties(
        boolean ssl,
        String username,
        boolean useEntraId,
        String password,
        String tenantId,
        String clientId,
        String clientSecret
    ) {
        Properties props = new Properties();
        props.setProperty("sslmode", ssl ? "require" : "prefer");
        if (username != null) {
            props.setProperty("user", username);
        }

        if (useEntraId) {
            props.setProperty("authenticationPluginClassName", ENTRA_ID_AUTH_PLUGIN);
            // Service principal creds are all optional: with none of them set, the plugin falls
            // back to DefaultAzureCredential (managed identity, environment variables, Azure CLI
            // login, etc.). Setting them switches to explicit service principal authentication
            // instead. Property keys per the shared azure-identity-extensions framework used by
            // both its MySQL and PostgreSQL plugins.
            if (tenantId != null) {
                props.setProperty("azure.tenantId", tenantId);
            }
            if (clientId != null) {
                props.setProperty("azure.clientId", clientId);
            }
            if (clientSecret != null) {
                props.setProperty("azure.clientSecret", clientSecret);
            }
        } else if (password != null) {
            props.setProperty("password", password);
        }

        return props;
    }

    /**
     * Builds the JDBC connection URL from individually-validated components rather than raw
     * string concatenation, so that a hostile {@code host} or {@code database} value (e.g.
     * containing {@code ?} or {@code #}) cannot smuggle extra driver parameters (such as
     * {@code socketFactory}) into the connection string. {@link URI}'s multi-argument
     * constructor percent-encodes reserved characters in the components it is given, and
     * rejects host values that are not syntactically valid hostnames.
     */
    static String buildJdbcUrl(String host, int port, String database) throws URISyntaxException {
        URI uri = new URI("postgresql", null, host, port, "/" + database, null, null);
        return "jdbc:" + uri;
    }

    protected abstract T run(RunContext runContext, Connection connection) throws Exception;

    /**
     * Registers the statement currently being executed so {@link #kill()} can cancel it if the
     * execution is killed or times out mid-query. Subclasses must call this immediately after
     * creating their {@link Statement} or {@link PreparedStatement}.
     */
    protected void trackStatement(Statement statement) {
        this.runningStatement = statement;
    }

    /**
     * Forces termination of the in-flight query: sends a cancel request to the server for the
     * currently tracked statement, then closes the connection. Invoked from a different thread
     * than the one running the task (see {@link io.kestra.core.models.WorkerJobLifecycle}), so
     * this must not rely on any thread-local or per-call state beyond the tracked fields.
     */
    @Override
    public void kill() {
        kill(this.runningStatement);
        kill(this.runningConnection);
    }

    private static void kill(Statement statement) {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.cancel();
                statement.close();
            }
        } catch (SQLException e) {
            // kill() must never throw or block: log and move on so kill(Connection) below still
            // runs even if cancelling the statement itself failed, rather than leaking the
            // connection because this exception aborted the rest of kill().
            log.warn("Failed to cancel in-flight HorizonDB statement", e);
        }
    }

    private static void kill(Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            log.warn("Failed to close HorizonDB connection during kill()", e);
        }
    }

    /**
     * Binds a nullable value onto a prepared statement parameter, falling back to a typed NULL
     * when the value is absent so drivers that reject untyped nulls (setObject(idx, null)) still
     * work. Defaults to {@link java.sql.Types#VARCHAR}; use the 4-arg overload for non-text
     * parameters (e.g. an integer), since a NULL bound with the wrong SQL type can cause
     * PostgreSQL to fail to resolve the correct function overload.
     */
    protected static void bind(PreparedStatement statement, int index, Object value) throws SQLException {
        bind(statement, index, value, java.sql.Types.VARCHAR);
    }

    protected static void bind(PreparedStatement statement, int index, Object value, int sqlType) throws SQLException {
        if (value == null) {
            statement.setNull(index, sqlType);
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
