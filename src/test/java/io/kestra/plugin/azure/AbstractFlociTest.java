package io.kestra.plugin.azure;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.time.Duration;

import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.storage.blob.BlobServiceClientBuilder;

import io.floci.testcontainers.az.FlociAzContainer;

// Singleton Floci Azure container shared by every emulator-backed test, as in plugin-aws. Ryuk removes it.
public abstract class AbstractFlociTest {
    public static final String CONTAINER = "tasks";
    public static final String FILE_SYSTEM = "tasks";
    public static final String TABLE = "tasks";
    public static final String CHECKPOINT_CONTAINER = "checkpoints";
    public static final String EVENT_HUB = "eh1";
    public static final String EVENT_HUB_NAMESPACE = "emulatorNs1";
    public static final String COSMOS_DATABASE = "cosmos-database-unit-test";
    public static final String COSMOS_CONTAINER = "cosmos-container-unit-test";

    // Public well-known Cosmos emulator key, the Java SDK requires one.
    public static final String COSMOS_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    private static final int EVENT_HUB_AMQP_PORT = freePort();
    private static final int EVENT_HUB_AMQPS_PORT = freePort();

    protected static final FlociAzContainer floci;

    static {
        floci = new FlociAzContainer()
            .withStartupTimeout(Duration.ofMinutes(5))
            // Auto mode makes Floci reach the Event Hubs sidecar by container IP, published mode would dial itself.
            .withEnv("FLOCI_AZ_DOCKER_ENDPOINT_MODE", "auto")
            .withTlsConfig(config -> config.enabled(true))
            .withEventHubConfig(
                config -> config
                    .mocked(false)
                    .kafkaEnabled(false)
                    .amqpPort(EVENT_HUB_AMQP_PORT)
                    .amqpTlsPort(EVENT_HUB_AMQPS_PORT)
            )
            // Only Blob, Table, Cosmos and Event Hubs are used, the other Docker-backed services would pull extra images.
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
        floci.start();
        try {
            trustFlociCertificate();
            startEventHubNamespace();
            provisionStorage();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to prepare the Floci Azure emulator", e);
        }
    }

    public static String cosmosConnectionString() {
        return "AccountEndpoint=%s/;AccountKey=%s;".formatted(floci.getHttpsEndpoint(), COSMOS_KEY);
    }

    public static String eventHubConnectionString() {
        return "Endpoint=sb://%s:%d;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=devkey;UseDevelopmentEmulator=true;EntityPath=%s;"
            .formatted(floci.getHost(), EVENT_HUB_AMQP_PORT, EVENT_HUB);
    }

    private static void provisionStorage() {
        var blobService = new BlobServiceClientBuilder()
            .connectionString(floci.getStorageConnectionString())
            .buildClient();
        blobService.createBlobContainerIfNotExists(CONTAINER);
        blobService.createBlobContainerIfNotExists(CHECKPOINT_CONTAINER);
        new TableServiceClientBuilder()
            .connectionString(floci.getStorageConnectionString())
            .buildClient()
            .createTableIfNotExists(TABLE);
    }

    // Namespaces start on demand, the PUT returns once the AMQP broker accepts connections.
    private static void startEventHubNamespace() throws Exception {
        var namespace = URI.create("%s/%s-eventhub/namespaces/%s".formatted(floci.getEndpoint(), floci.getAccountName(), EVENT_HUB_NAMESPACE));
        // The Java EventProcessorClient lowercases $Default and Floci keeps both spellings apart, so both groups must exist.
        var body = """
            {"amqpPort":%d,"amqpTlsPort":%d,"entities":"%s:4","consumerGroups":"$Default,$default"}"""
            .formatted(EVENT_HUB_AMQP_PORT, EVENT_HUB_AMQPS_PORT, EVENT_HUB);
        var response = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder(namespace)
                .timeout(Duration.ofMinutes(3))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
        if (response.statusCode() != 200 && response.statusCode() != 201) {
            throw new IllegalStateException("Event Hubs namespace did not start: HTTP %d %s".formatted(response.statusCode(), response.body()));
        }
    }

    // Cosmos only speaks HTTPS, so the JVM truststore gets the emulator certificate added to the default CAs.
    private static void trustFlociCertificate() throws Exception {
        var certificate = CertificateFactory.getInstance("X.509")
            .generateCertificate(new ByteArrayInputStream(floci.getTlsCertificate().getBytes(StandardCharsets.UTF_8)));

        var keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (var in = Files.newInputStream(Path.of(System.getProperty("java.home"), "lib", "security", "cacerts"))) {
            keyStore.load(in, "changeit".toCharArray());
        }
        keyStore.setCertificateEntry("floci-az", certificate);

        var trustStore = Files.createTempFile("floci-az-trust-", ".p12");
        trustStore.toFile().deleteOnExit();
        try (var out = Files.newOutputStream(trustStore)) {
            keyStore.store(out, "changeit".toCharArray());
        }
        System.setProperty("javax.net.ssl.trustStore", trustStore.toAbsolutePath().toString());
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");
        System.setProperty("javax.net.ssl.trustStoreType", KeyStore.getDefaultType());
    }

    private static int freePort() {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Could not reserve a port for the Event Hubs emulator", e);
        }
    }
}
