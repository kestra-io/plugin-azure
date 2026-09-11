package io.kestra.plugin.azure.monitoring;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.zip.GZIPInputStream;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.util.Context;
import com.azure.monitor.ingestion.LogsIngestionClient;
import com.azure.monitor.ingestion.LogsIngestionClientBuilder;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Builds a real LogsIngestionClient over a transport that records the request instead of sending it, so tests
 * assert on what the SDK actually puts on the wire.
 */
final class IngestionStub implements com.azure.core.http.HttpClient {
    private final int status;
    private volatile HttpRequest request;
    private volatile byte[] body;

    private IngestionStub(int status) {
        this.status = status;
    }

    static IngestionStub respondingWith(int status) {
        return new IngestionStub(status);
    }

    LogsIngestionClient client(String endpoint) {
        TokenCredential credential = context -> Mono.just(new AccessToken("stub-token", OffsetDateTime.now().plusHours(1)));

        return new LogsIngestionClientBuilder()
            .credential(credential)
            .endpoint(endpoint)
            .httpClient(this)
            .buildClient();
    }

    String requestUrl() {
        return request.getUrl().toString();
    }

    String requestMethod() {
        return request.getHttpMethod().toString();
    }

    /** The SDK gzips the payload, so decompress when it says it did. */
    String requestBody() throws Exception {
        if ("gzip".equalsIgnoreCase(request.getHeaders().getValue(com.azure.core.http.HttpHeaderName.CONTENT_ENCODING))) {
            try (var gzip = new GZIPInputStream(new ByteArrayInputStream(body))) {
                return new String(gzip.readAllBytes(), StandardCharsets.UTF_8);
            }
        }

        return new String(body, StandardCharsets.UTF_8);
    }

    boolean gzipped() {
        return "gzip".equalsIgnoreCase(request.getHeaders().getValue(com.azure.core.http.HttpHeaderName.CONTENT_ENCODING));
    }

    @Override
    public Mono<HttpResponse> send(HttpRequest request) {
        return Mono.just(capture(request));
    }

    @Override
    public Mono<HttpResponse> send(HttpRequest request, Context context) {
        return Mono.just(capture(request));
    }

    @Override
    public HttpResponse sendSync(HttpRequest request, Context context) {
        return capture(request);
    }

    private HttpResponse capture(HttpRequest request) {
        this.request = request;
        this.body = request.getBodyAsBinaryData() == null ? new byte[0] : request.getBodyAsBinaryData().toBytes();

        return new HttpResponse(request) {
            @Override
            public int getStatusCode() {
                return status;
            }

            @Override
            public String getHeaderValue(String name) {
                return null;
            }

            @Override
            public HttpHeaders getHeaders() {
                return new HttpHeaders();
            }

            @Override
            public Flux<ByteBuffer> getBody() {
                return Flux.empty();
            }

            @Override
            public Mono<byte[]> getBodyAsByteArray() {
                return Mono.just(new byte[0]);
            }

            @Override
            public Mono<String> getBodyAsString() {
                return Mono.just("");
            }

            @Override
            public Mono<String> getBodyAsString(java.nio.charset.Charset charset) {
                return Mono.just("");
            }
        };
    }
}
