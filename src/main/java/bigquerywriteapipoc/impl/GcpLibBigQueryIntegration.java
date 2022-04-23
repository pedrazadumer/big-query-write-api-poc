package bigquerywriteapipoc.impl;

import bigquerywriteapipoc.BigQueryIntegration;
import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.protobuf.Descriptors;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * BigQuery integration using the GCP-provided library, which internally
 * uses GRPC as much as possible to optimize the performance.
 *
 * This was created using the example from the GCP BigQuery documentation:
 * https://cloud.google.com/bigquery/docs/write-api#committed
 */
public class GcpLibBigQueryIntegration implements BigQueryIntegration<JSONObject> {


    @Override
    public void writeRecord(JSONObject record, String projectId, String datasetName, String tableName) {
        try (BigQueryWriteClient client = BigQueryWriteClient.create()) {
            WriteStream writeStream = buildWriteStream(client, projectId, datasetName, tableName);
            try (JsonStreamWriter writer =
                         JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema())
                                 .build()) {
                // From GCP docs: A writer should be used for as much writes as possible. Creating a writer
                // for just one write is an antipattern. We write a single record here because it's just a PoC.
                JSONArray recordArray = new JSONArray();
                recordArray.put(record);

                ApiFuture<AppendRowsResponse> future = writer.append(recordArray);
                AppendRowsResponse response = future.get();

                // Finalize the stream after use.
                FinalizeWriteStreamRequest finalizeWriteStreamRequest =
                        FinalizeWriteStreamRequest.newBuilder().setName(writeStream.getName()).build();
                client.finalizeWriteStream(finalizeWriteStreamRequest);
            }
            System.out.println("Wrote record successfully to GCP BigQuery.");
        } catch (InterruptedException | ExecutionException | Descriptors.DescriptorValidationException | IOException e) {
            throw new RuntimeException("Error writing record to GCP BigQuery.", e);
        }
    }

    protected static WriteStream buildWriteStream(BigQueryWriteClient client, String projectId, String datasetName, String tableName) {
        // Initialize a write stream for the specified table.
        // For more information on WriteStream.Type, see:
        // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1beta2/WriteStream.Type.html
        WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();
        TableName parentTable = TableName.of(projectId, datasetName, tableName);
        CreateWriteStreamRequest createWriteStreamRequest =
                CreateWriteStreamRequest.newBuilder()
                        .setParent(parentTable.toString())
                        .setWriteStream(stream)
                        .build();
        return client.createWriteStream(createWriteStreamRequest);
    }
}
