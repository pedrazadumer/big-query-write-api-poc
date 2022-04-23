package bigquerywriteapipoc;

import bigquerywriteapipoc.impl.GcpLibBigQueryIntegration;
import org.json.JSONObject;

public class BigQueryWriteApiPocApplication {

    public static final String PROJECT_ID = "";
    public static final String DATASET_NAME = "";
    public static final String TABLE_NAME = "";

    public static void main(String[] args) {

        BigQueryIntegration<JSONObject> integration = new GcpLibBigQueryIntegration();

        JSONObject record = new JSONObject();
        record.put("saleNumber", "S00015");
        record.put("creationDate", "2022-04-23 23:45:38");
        record.put("customer", "Gemini Furniture");
        record.put("salesperson", "Marc Demo");
        record.put("total", 8287.5f);
        record.put("currency", "EUR");

        integration.writeRecord(record, PROJECT_ID, DATASET_NAME, TABLE_NAME);
    }
}
