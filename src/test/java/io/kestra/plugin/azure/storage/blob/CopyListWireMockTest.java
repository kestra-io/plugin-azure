package io.kestra.plugin.azure.storage.blob;

import java.util.Base64;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.storage.blob.models.CopyListDestination;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class CopyListWireMockTest {
    private static final String ACCOUNT = "devstoreaccount1";

    @Inject
    private RunContextFactory runContextFactory;

    private WireMockServer server;

    @BeforeEach
    void start() {
        server = new WireMockServer(options().dynamicPort());
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop();
    }

    private String listing(String... names) {
        var blobs = new StringBuilder();
        for (var name : names) {
            blobs.append("<Blob><Name>").append(name).append("</Name><Properties>")
                .append("<Content-Length>5</Content-Length><Content-Type>text/csv</Content-Type><BlobType>BlockBlob</BlobType>")
                .append("<Last-Modified>Tue, 01 Jan 2030 00:00:00 GMT</Last-Modified><Etag>0x1</Etag>")
                .append("</Properties></Blob>");
        }
        return "<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults ServiceEndpoint=\"" + server.baseUrl() + "/" + ACCOUNT +
            "/\" ContainerName=\"src\"><Blobs>" + blobs + "</Blobs><NextMarker /></EnumerationResults>";
    }

    private void stubListing(String... names) {
        server.stubFor(get(urlPathEqualTo("/" + ACCOUNT + "/src"))
            .withQueryParam("comp", equalTo("list"))
            .willReturn(ok(listing(names)).withHeader("Content-Type", "application/xml")));
    }

    private void stubCopyAndDelete() {
        server.stubFor(put(urlPathMatching("/" + ACCOUNT + "/dst/.*"))
            .willReturn(aResponse().withStatus(202)
                .withHeader("x-ms-copy-id", "copy-id")
                .withHeader("x-ms-copy-status", "success")
                .withHeader("ETag", "\"0x1\"")
                .withHeader("Last-Modified", "Tue, 01 Jan 2030 00:00:00 GMT")));
        server.stubFor(head(urlPathMatching("/" + ACCOUNT + "/dst/.*"))
            .willReturn(aResponse().withStatus(200)
                .withHeader("x-ms-copy-id", "copy-id")
                .withHeader("x-ms-copy-status", "success")
                .withHeader("x-ms-copy-source", server.baseUrl() + "/source")
                .withHeader("x-ms-copy-completion-time", "Tue, 01 Jan 2030 00:00:00 GMT")
                .withHeader("ETag", "\"0x1\"")
                .withHeader("Last-Modified", "Tue, 01 Jan 2030 00:00:00 GMT")));
        server.stubFor(delete(urlPathMatching("/" + ACCOUNT + "/src/.*")).willReturn(aResponse().withStatus(202)));
    }

    // The SDK percent-encodes slashes inside blob names
    private static String path(String container, String blob) {
        return "/" + ACCOUNT + "/" + container + "/" + blob.replace("/", "%2F");
    }

    private CopyList task(boolean delete) {
        var key = Base64.getEncoder().encodeToString("test-account-key".getBytes());
        return CopyList.builder()
            .id(IdUtils.create())
            .type(CopyList.class.getName())
            .connectionString(Property.ofValue(
                "DefaultEndpointsProtocol=http;AccountName=" + ACCOUNT + ";AccountKey=" + key + ";BlobEndpoint=" + server.baseUrl() + "/" + ACCOUNT
            ))
            .container(Property.ofValue("src"))
            .prefix(Property.ofValue("a/"))
            .to(CopyListDestination.builder().container(Property.ofValue("dst")).prefix(Property.ofValue("z/")).build())
            .delete(Property.ofValue(delete))
            .build();
    }

    @Test
    void shouldCopyAndDeleteListedBlobs() throws Exception {
        stubListing("a/one.csv", "a/sub/two.csv");
        stubCopyAndDelete();

        var output = task(true).run(runContextFactory.of());

        assertThat(output.getCount(), is(2L));
        assertThat(output.getSize(), is(10L));
        server.verify(1, putRequestedFor(urlPathEqualTo(path("dst", "z/one.csv"))));
        server.verify(1, putRequestedFor(urlPathEqualTo(path("dst", "z/sub/two.csv"))));
        server.verify(1, deleteRequestedFor(urlPathEqualTo(path("src", "a/one.csv"))));
        server.verify(1, deleteRequestedFor(urlPathEqualTo(path("src", "a/sub/two.csv"))));
    }

    @Test
    void shouldCopyWithoutDeletingSource() throws Exception {
        stubListing("a/one.csv");
        stubCopyAndDelete();

        var output = task(false).run(runContextFactory.of());

        assertThat(output.getCount(), is(1L));
        server.verify(1, putRequestedFor(urlPathEqualTo(path("dst", "z/one.csv"))));
        server.verify(0, deleteRequestedFor(urlPathMatching("/.*")));
    }

    @Test
    void shouldDoNothingWhenNoBlobMatches() throws Exception {
        stubListing();
        stubCopyAndDelete();

        var output = task(true).run(runContextFactory.of());

        assertThat(output.getCount(), is(0L));
        assertThat(output.getSize(), is(0L));
        server.verify(0, putRequestedFor(urlPathMatching("/.*")));
        server.verify(0, deleteRequestedFor(urlPathMatching("/.*")));
    }
}
