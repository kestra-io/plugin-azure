package io.kestra.plugin.azure.devops.pipelines;

import java.time.Instant;

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
public class PipelineRun {
    @Schema(
        title = "Run ID",
        description = """
            The unique identifier of the pipeline run.
            """
    )
    private final Integer id;

    @Schema(
        title = "Run name",
        description = """
            The name or build number of the pipeline run.
            """
    )
    private final String name;

    @Schema(
        title = "Run state",
        description = """
            Current state of the pipeline run (e.g. inProgress, completed, canceling, notStarted).
            """
    )
    private final String state;

    @Schema(
        title = "Run result",
        description = """
            Final outcome of the pipeline run (e.g. succeeded, failed, canceled).
            """
    )
    private final String result;

    @Schema(
        title = "Creation timestamp",
        description = """
            Timestamp when the pipeline run was created.
            """
    )
    private final Instant createdDate;

    @Schema(
        title = "Finish timestamp",
        description = """
            Timestamp when the pipeline run finished executing.
            """
    )
    private final Instant finishedDate;

    @Schema(
        title = "API URL",
        description = """
            REST API URL of the pipeline run resource.
            """
    )
    private final String url;

    @Schema(
        title = "Web URL",
        description = """
            Azure DevOps browser URL to view the pipeline run execution.
            """
    )
    private final String webUrl;

    public static PipelineRun fromJson(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }

        Integer id = node.hasNonNull("id") ? node.get("id").asInt() : null;
        String name = node.hasNonNull("name") ? node.get("name").asText() : null;
        String state = node.hasNonNull("state") ? node.get("state").asText() : null;
        String result = node.hasNonNull("result") ? node.get("result").asText() : null;
        Instant createdDate = node.hasNonNull("createdDate") ? parseInstant(node.get("createdDate").asText()) : null;
        Instant finishedDate = node.hasNonNull("finishedDate") ? parseInstant(node.get("finishedDate").asText()) : null;
        String url = node.hasNonNull("url") ? node.get("url").asText() : null;

        String webUrl = null;
        if (node.hasNonNull("_links") && node.get("_links").hasNonNull("web") && node.get("_links").get("web").hasNonNull("href")) {
            webUrl = node.get("_links").get("web").get("href").asText();
        }

        return PipelineRun.builder()
            .id(id)
            .name(name)
            .state(state)
            .result(result)
            .createdDate(createdDate)
            .finishedDate(finishedDate)
            .url(url)
            .webUrl(webUrl)
            .build();
    }

    private static Instant parseInstant(String text) {
        try {
            return Instant.parse(text);
        } catch (Exception e) {
            return null;
        }
    }
}
