package io.kestra.plugin.azure.storage.table;

import com.azure.data.tables.models.TableErrorCode;
import com.azure.data.tables.models.TableServiceException;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigInteger;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class SuiteTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${kestra.variables.globals.azure.tables.endpoint}")
    protected String endpoint;

    @Value("${kestra.variables.globals.azure.blobs.connection-string}")
    protected String connectionString;

    @Value("${kestra.variables.globals.azure.tables.table}")
    protected String table;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        String partitionKey = IdUtils.create();
        String rowKey = IdUtils.create();
        UUID uuid = UUID.randomUUID();

        // data
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);
        for (int i = 0; i < 50; i++) {
            HashMap<Object, Object> data = new HashMap<>();
            data.put("partitionKey", partitionKey);
            data.put("rowKey", i == 0 ? rowKey : IdUtils.create());
            data.put("properties", Map.of(
                "int32", 123456789,
                "int64", 123456789123456789L,
                "string", "s",
                "bool", true,
                "double", 2147483645.1234d,
                "guid", uuid,
                "datetime", ZonedDateTime.parse("2007-05-08T12:35+02:00[Europe/Paris]"),
                "binary", new BigInteger("DEADBEEF", 16).toByteArray()
            ));

            FileSerde.write(output, data);
        }
        URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        // create
        Bulk bulk = Bulk.builder()
            .endpoint(Property.of(this.endpoint))
            .connectionString(Property.of(connectionString))
            .table(Property.of(this.table))
            .from(uri.toString())
            .build();

        Bulk.Output bulkOutput = bulk.run(runContext);
        assertThat(bulkOutput.getCount(), is(50));

        // get
        Get get = Get.builder()
            .endpoint(Property.of(this.endpoint))
            .connectionString(Property.of(connectionString))
            .table(Property.of(this.table))
            .partitionKey(Property.of(partitionKey))
            .rowKey(Property.of(rowKey))
            .build();

        Get.Output getOutput = get.run(runContext);
        assertThat(getOutput.getRow().getProperties().get("int32"), is(123456789));
        assertThat(getOutput.getRow().getProperties().get("int64"), is(123456789123456789L));
        assertThat(getOutput.getRow().getProperties().get("string"), is("s"));
        assertThat(getOutput.getRow().getProperties().get("bool"), is(true));
        assertThat(getOutput.getRow().getProperties().get("double"), is(2147483645.1234d));
        assertThat(getOutput.getRow().getProperties().get("guid"), is(ByteBuffer.allocate(2 * Long.BYTES).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array()));
        assertThat(getOutput.getRow().getProperties().get("datetime"), is(OffsetDateTime.parse("2007-05-08T10:35Z")));
        assertThat(getOutput.getRow().getProperties().get("binary"), is(new BigInteger("DEADBEEF", 16).toByteArray()));

        // list
        List list = List.builder()
            .endpoint(Property.of(this.endpoint))
            .connectionString(Property.of(connectionString))
            .table(Property.of(this.table))
            .filter(Property.of("PartitionKey eq '" + partitionKey + "'"))
            .build();

        List.Output listOutput = list.run(runContext);

        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(null, null, listOutput.getUri())));
        java.util.List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));

        assertThat(listOutput.getCount(), is(50L));
        assertThat(result.size(), is(50));
        assertThat(((Map<String, Object>) result.get(0).get("properties")).get("int32"), is(123456789));

        // delete
        Delete delete = Delete.builder()
            .endpoint(Property.of(this.endpoint))
            .connectionString(Property.of(connectionString))
            .table(Property.of(this.table))
            .partitionKey(Property.of(partitionKey))
            .rowKey(Property.of(rowKey))
            .build();

        delete.run(runContext);

        // is deleted
        TableServiceException e = assertThrows(TableServiceException.class, () -> {
            get.run(runContext);
        });

        assertThat(e.getValue().getErrorCode(), is(TableErrorCode.RESOURCE_NOT_FOUND));
    }
}
