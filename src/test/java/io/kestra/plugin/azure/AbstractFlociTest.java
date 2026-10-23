package io.kestra.plugin.azure;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.storage.blob.BlobServiceClientBuilder;

import io.floci.testcontainers.az.FlociAzContainer;

/**
 * One Floci Azure container for every integration test that can run on the emulator.
 *
 * <p>
 * Same singleton shape as {@code io.kestra.plugin.aws.AbstractFlociTest}: the container starts
 * once in a static initializer, Ryuk removes it, and subclasses read endpoints from here instead of
 * a real Azure account. {@code @KestraTest} stays on the concrete tests because they do not share
 * one runner configuration. {@code @Testcontainers} is omitted: Micronaut's annotation processor
 * rejects that annotation on this classpath.
 */
public abstract class AbstractFlociTest {
    public static final String CONTAINER = "tasks";
    public static final String FILE_SYSTEM = "tasks";
    public static final String TABLE = "tasks";
    public static final String CHECKPOINT_CONTAINER = "checkpoints";
    public static final String EVENT_HUB = "eh1";
    public static final String EVENT_HUB_NAMESPACE = "emulatorNs1";
    public static final String COSMOS_DATABASE = "cosmos-database-unit-test";
    public static final String COSMOS_CONTAINER = "cosmos-container-unit-test";

    /**
     * Well-known Cosmos emulator key. Floci accepts it in dev mode; the Java SDK still requires a key.
     */
    public static final String COSMOS_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    private static final AtomicBoolean STARTED = new AtomicBoolean(false);
    private static final int EVENT_HUB_AMQP_PORT = freePort();
    private static final int EVENT_HUB_AMQPS_PORT = freePort();

    protected static final FlociAzContainer floci;

    static {
        // Reactor Netty must use the JDK SSL provider so it honors the truststore installed below.
        System.setProperty("io.netty.handler.ssl.noOpenSsl", "true");
        floci = new FlociAzContainer()
            .withStartupTimeout(Duration.ofMinutes(5))
            // Published mode addresses sidecars as localhost. Inside this container that is
            // the emulator itself, so the Artemis readiness check never sees the published port.
            // Auto mode makes Floci dial the sidecar's container IP, while the port stays
            // published on the Docker host for the test process.
            .withEnv("FLOCI_AZ_DOCKER_ENDPOINT_MODE", "auto")
            .withTlsConfig(config -> config.enabled(true))
            .withEventHubConfig(
                config -> config
                    .mocked(false)
                    .kafkaEnabled(false)
                    .amqpPort(EVENT_HUB_AMQP_PORT)
                    .amqpTlsPort(EVENT_HUB_AMQPS_PORT)
            )
            // Keep startup to Blob, Table, Cosmos and Event Hubs. The other Docker-backed
            // services are emulated but unused by these tests and pull extra images.
            .withFunctionsConfig(config -> config.enabled(false))
            .withSqlConfig(config -> config.enabled(false))
            .withPostgresConfig(config -> config.enabled(false))
            .withMySqlConfig(config -> config.enabled(false))
            .withMariaDbConfig(config -> config.enabled(false))
            .withAksConfig(config -> config.enabled(false))
            .withRedisConfig(config -> config.enabled(false))
            .withAcrConfig(config -> config.enabled(false))
            .withAciConfig(config -> config.enabled(false))
            .withVmConfig(config -> config.enabled(false))
            .withContainerAppsConfig(config -> config.enabled(false))
            .withServiceBusConfig(config -> config.mocked(true));
        ensureStarted();
    }

    public static void ensureStarted() {
        if (STARTED.get()) {
            return;
        }
        synchronized (AbstractFlociTest.class) {
            if (STARTED.get()) {
                return;
            }
            floci.start();
            try {
                trustFlociCertificate();
                waitForEventHub();
                provisionStorage();
                publishConfiguration();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to prepare the Floci Azure emulator", e);
            }
            STARTED.set(true);
        }
    }

    public static String blobEndpoint() {
        return floci.getBlobEndpoint();
    }

    public static String tableEndpoint() {
        return floci.getTableEndpoint();
    }

    public static String storageConnectionString() {
        return floci.getStorageConnectionString();
    }

    public static String cosmosEndpoint() {
        return floci.getHttpsEndpoint();
    }

    public static String cosmosConnectionString() {
        return "AccountEndpoint=" + cosmosEndpoint() + "/;AccountKey=" + COSMOS_KEY + ";";
    }

    public static String eventHubConnectionString() {
        return "Endpoint=sb://" + floci.getHost() + ":" + EVENT_HUB_AMQP_PORT
            + ";SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=devkey;"
            + "UseDevelopmentEmulator=true;EntityPath=" + EVENT_HUB + ";";
    }

    private static void publishConfiguration() {
        set("kestra.variables.globals.azure.blobs.endpoint", blobEndpoint());
        set("kestra.variables.globals.azure.blobs.connection-string", storageConnectionString());
        set("kestra.variables.globals.azure.blobs.container", CONTAINER);
        set("kestra.variables.globals.azure.tables.endpoint", tableEndpoint());
        set("kestra.variables.globals.azure.tables.table", TABLE);
        set("kestra.variables.globals.azure.adls.endpoint", blobEndpoint());
        set("kestra.variables.globals.azure.adls.file-system", FILE_SYSTEM);
        set("kestra.variables.globals.azure.cosmos.endpoint", cosmosEndpoint());
        set("kestra.variables.globals.azure.cosmos.connection-string", cosmosConnectionString());
        set("kestra.variables.globals.azure.cosmos.database-id", COSMOS_DATABASE);
        set("kestra.variables.globals.azure.cosmos.container-id", COSMOS_CONTAINER);
        set("kestra.variables.globals.azure.eventhubs.eventhub-name", EVENT_HUB);
        set("kestra.variables.globals.azure.eventhubs.namespace", EVENT_HUB_NAMESPACE);
        set("kestra.variables.globals.azure.eventhubs.connection-string", eventHubConnectionString());
        set("kestra.variables.globals.azure.eventhubs.checkpointstore.connection-string", storageConnectionString());
        set("kestra.variables.globals.azure.eventhubs.checkpointstore.container-name", CHECKPOINT_CONTAINER);
    }

    private static void set(String key, String value) {
        System.setProperty(key, value);
    }

    private static void provisionStorage() {
        new BlobServiceClientBuilder()
            .connectionString(storageConnectionString())
            .buildClient()
            .createBlobContainerIfNotExists(CONTAINER);
        new BlobServiceClientBuilder()
            .connectionString(storageConnectionString())
            .buildClient()
            .createBlobContainerIfNotExists(CHECKPOINT_CONTAINER);
        new TableServiceClientBuilder()
            .connectionString(storageConnectionString())
            .buildClient()
            .createTableIfNotExists(TABLE);
    }

    private static void waitForEventHub() throws Exception {
        // Namespaces start on demand. The PUT blocks until the Artemis sidecar accepts AMQP.
        URI namespace = URI.create(
            floci.getEndpoint() + "/" + floci.getAccountName()
                + "-eventhub/namespaces/" + EVENT_HUB_NAMESPACE
        );
        String body = "{\"amqpPort\":" + EVENT_HUB_AMQP_PORT
            + ",\"amqpTlsPort\":" + EVENT_HUB_AMQPS_PORT
            // The Java EventProcessorClient lowercases the consumer group ($Default -> $default).
            // Floci keeps those as separate groups, so both must exist or the processor sees nothing.
            + ",\"entities\":\"eh1:4\",\"consumerGroups\":\"$Default,$default\"}";
        HttpResponse<String> response = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder(namespace)
                .timeout(Duration.ofMinutes(3))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
        if (response.statusCode() != 200 && response.statusCode() != 201) {
            throw new IllegalStateException(
                "Event Hubs namespace did not start: HTTP " + response.statusCode() + " " + response.body()
            );
        }
    }

    private static void trustFlociCertificate() throws Exception {
        String pem = floci.getTlsCertificate();
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        X509Certificate certificate = (X509Certificate) factory.generateCertificate(
            new ByteArrayInputStream(pem.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        Path cacerts = Path.of(System.getProperty("java.home"), "lib", "security", "cacerts");
        try (InputStream in = Files.newInputStream(cacerts)) {
            keyStore.load(in, "changeit".toCharArray());
        }
        keyStore.setCertificateEntry("floci-az", certificate);

        Path trustStore = Files.createTempFile("floci-az-trust-", ".p12");
        trustStore.toFile().deleteOnExit();
        try (OutputStream out = Files.newOutputStream(trustStore)) {
            keyStore.store(out, "changeit".toCharArray());
        }
        System.setProperty("javax.net.ssl.trustStore", trustStore.toAbsolutePath().toString());
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");
        System.setProperty("javax.net.ssl.trustStoreType", KeyStore.getDefaultType());
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Could not reserve a port for the Event Hubs emulator", e);
        }
    }
}
