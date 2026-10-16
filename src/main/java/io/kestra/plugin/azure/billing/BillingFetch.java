package io.kestra.plugin.azure.billing;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

/** Applies a {@link FetchType} to rows as they are read, so STORE streams each page to an ION file. */
final class BillingFetch implements AutoCloseable {
    private final RunContext runContext;
    private final FetchType fetchType;
    private final List<Map<String, Object>> rows = new ArrayList<>();
    private Map<String, Object> first;
    private File file;
    private OutputStream out;
    private long size;

    record Result(List<Map<String, Object>> rows, Map<String, Object> row, URI uri, long size) {
    }

    private BillingFetch(RunContext runContext, FetchType fetchType) {
        this.runContext = runContext;
        this.fetchType = fetchType;
    }

    static BillingFetch open(RunContext runContext, FetchType fetchType) throws IOException {
        var fetch = new BillingFetch(runContext, fetchType);
        if (fetchType == FetchType.STORE) {
            fetch.file = runContext.workingDir().createTempFile(".ion").toFile();
            fetch.out = new BufferedOutputStream(new FileOutputStream(fetch.file), FileSerde.BUFFER_SIZE);
        }
        return fetch;
    }

    void add(Map<String, Object> row) throws IOException {
        size++;
        switch (fetchType) {
            case FETCH -> rows.add(row);
            case FETCH_ONE -> {
                if (first == null) {
                    first = row;
                }
            }
            case STORE -> FileSerde.write(out, row);
            case NONE -> {
            }
        }
    }

    Result finish() throws IOException {
        Result result = switch (fetchType) {
            case FETCH -> new Result(rows, null, null, size);
            case FETCH_ONE -> new Result(null, first, null, first == null ? 0 : 1);
            case STORE -> {
                out.close();
                out = null;
                yield new Result(null, null, runContext.storage().putFile(file), size);
            }
            case NONE -> new Result(null, null, null, size);
        };
        runContext.metric(Counter.of("fetch.rows", result.size()));
        return result;
    }

    @Override
    public void close() throws IOException {
        if (out != null) {
            out.close();
        }
    }
}
