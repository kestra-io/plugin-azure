package io.kestra.plugin.azure.devops.pipelines;

import com.fasterxml.jackson.databind.JsonNode;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
@EqualsAndHashCode
public class Pipeline {
    @Schema(
        title = "Pipeline ID",
        description = """
            The unique identifier of the pipeline definition.
            """
    )
    private final Integer id;

    @Schema(
        title = "Pipeline name",
        description = """
            The name of the pipeline definition.
            """
    )
    private final String name;

    @Schema(
        title = "Pipeline folder",
        description = """
            The folder containing the pipeline definition.
            """
    )
    private final String folder;

    @Schema(
        title = "Revision number",
        description = """
            The revision number of the pipeline definition.
            """
    )
    private final Integer revision;

    @Schema(
        title = "API URL",
        description = """
            REST API URL of the pipeline definition resource.
            """
    )
    private final String url;

    @Schema(
        title = "Web URL",
        description = """
            Azure DevOps browser URL to view the pipeline definition.
            """
    )
    private final String webUrl;

    public static Pipeline fromJson(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }

        Integer id = node.hasNonNull("id") ? node.get("id").asInt() : null;
        String name = node.hasNonNull("name") ? node.get("name").asText() : null;
        String folder = node.hasNonNull("folder") ? node.get("folder").asText() : null;
        Integer revision = node.hasNonNull("revision") ? node.get("revision").asInt() : null;
        String url = node.hasNonNull("url") ? node.get("url").asText() : null;

        String webUrl = null;
        if (node.hasNonNull("_links") && node.get("_links").hasNonNull("web") && node.get("_links").get("web").hasNonNull("href")) {
            webUrl = node.get("_links").get("web").get("href").asText();
        }

        return Pipeline.builder()
            .id(id)
            .name(name)
            .folder(folder)
            .revision(revision)
            .url(url)
            .webUrl(webUrl)
            .build();
    }
}
