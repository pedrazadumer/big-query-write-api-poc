package bigquerywriteapipoc;

public interface BigQueryIntegration<T> {

    void writeRecord(T record, String projectId, String datasetName, String tableName);

}
