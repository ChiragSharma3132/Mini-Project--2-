package backend.dbengine;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class StorageManager {

    // Central metadata collection tracks schema for each logical SQL table.
    private static final String META_COLLECTION = "_table_meta";

    private MongoCollection<Document> metaCollection() {
        return DBConnection.getDatabase().getCollection(META_COLLECTION);
    }

    private MongoCollection<Document> dataCollection(String tableName) {
        // Prefix keeps user tables separate from internal metadata collections.
        return DBConnection.getDatabase().getCollection("tbl_" + tableName);
    }

    private Document getTableMeta(String tableName) {
        return metaCollection().find(Filters.eq("tableName", tableName)).first();
    }

    private Document getResolvedTableMeta(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            return null;
        }

        Document exact = getTableMeta(tableName);
        if (exact != null) {
            return exact;
        }

        String quotedName = "^" + Pattern.quote(tableName.trim()) + "$";
        return metaCollection().find(Filters.regex("tableName", quotedName, "i")).first();
    }

    public List<String> getTableNames() {
        List<String> names = new ArrayList<>();
        for (Document meta : metaCollection().find()) {
            String tableName = meta.getString("tableName");
            if (tableName != null && !tableName.isBlank()) {
                names.add(tableName);
            }
        }
        return names;
    }

    public void createTable(String tableName, String[] columns, String[] types) {
        if (getResolvedTableMeta(tableName) != null) {
            System.out.println("Table already exists");
            return;
        }

        Document meta = new Document("tableName", tableName)
                .append("columns", List.of(columns))
                .append("types", List.of(types));

        metaCollection().insertOne(meta);

        // Touch the data collection so it appears immediately in Atlas.
        dataCollection(tableName).insertOne(new Document("_init", true));
        dataCollection(tableName).deleteOne(Filters.eq("_init", true));
    }

    public boolean insertRow(String tableName, String[] values) {
        Document meta = getResolvedTableMeta(tableName);
        if (meta == null) {
            System.out.println("Table not found: " + tableName);
            return false;
        }

        String canonicalTableName = meta.getString("tableName");
        if (canonicalTableName == null || canonicalTableName.isBlank()) {
            System.out.println("Table not found: " + tableName);
            return false;
        }

        List<String> columns = meta.getList("columns", String.class);
        if (columns == null || columns.isEmpty()) {
            return false;
        }

        Document row = new Document();
        for (int i = 0; i < columns.size(); i++) {
            String col = columns.get(i);
            String val = i < values.length ? values[i] : "";
            row.append(col, val);
        }

        dataCollection(canonicalTableName).insertOne(row);
        return true;
    }

    public List<String[]> readTable(String tableName) {
        List<String[]> rows = new ArrayList<>();
        Document meta = getResolvedTableMeta(tableName);
        if (meta == null) {
            System.out.println("Table not found: " + tableName);
            return rows;
        }

        String canonicalTableName = meta.getString("tableName");
        if (canonicalTableName == null || canonicalTableName.isBlank()) {
            System.out.println("Table not found: " + tableName);
            return rows;
        }

        List<String> columns = meta.getList("columns", String.class);
        if (columns == null || columns.isEmpty()) {
            return rows;
        }

        // Keep compatibility with existing QueryExecutor logic: first row is header.
        rows.add(columns.toArray(new String[0]));

        for (Document doc : dataCollection(canonicalTableName).find()) {
            String[] row = new String[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                Object value = doc.get(columns.get(i));
                row[i] = value == null ? "" : value.toString();
            }
            rows.add(row);
        }

        return rows;
    }

    public int updateRows(String tableName, Map<String, String> updates, String whereColumn, String whereValue) {
        Document meta = getResolvedTableMeta(tableName);
        if (meta == null) return 0;

        String canonicalTableName = meta.getString("tableName");
        if (canonicalTableName == null || canonicalTableName.isBlank()) return 0;

        Bson filter = Filters.eq(whereColumn, whereValue);

        List<Bson> updateOps = new ArrayList<>();
        for (Map.Entry<String, String> entry : updates.entrySet()) {
            updateOps.add(Updates.set(entry.getKey(), entry.getValue()));
        }

        if (updateOps.isEmpty()) return 0;

        UpdateResult result = dataCollection(canonicalTableName).updateMany(filter, Updates.combine(updateOps));
        return (int) result.getModifiedCount();
    }

    public int deleteRows(String tableName, String whereColumn, String whereValue) {
        Document meta = getResolvedTableMeta(tableName);
        if (meta == null) return 0;

        String canonicalTableName = meta.getString("tableName");
        if (canonicalTableName == null || canonicalTableName.isBlank()) return 0;

        DeleteResult result = dataCollection(canonicalTableName).deleteMany(Filters.eq(whereColumn, whereValue));
        return (int) result.getDeletedCount();
    }

    public void dropTable(String tableName) {
        Document meta = getResolvedTableMeta(tableName);
        if (meta == null) {
            return;
        }

        String canonicalTableName = meta.getString("tableName");
        if (canonicalTableName == null || canonicalTableName.isBlank()) {
            return;
        }

        metaCollection().deleteOne(Filters.eq("tableName", canonicalTableName));
        dataCollection(canonicalTableName).drop();
    }

    public String[] getTableTypes(String tableName) {
        Document meta = getResolvedTableMeta(tableName);
        if (meta == null) {
            return new String[0];
        }

        List<String> types = meta.getList("types", String.class);
        if (types == null) {
            return new String[0];
        }

        return types.toArray(new String[0]);
    }
// bcdnvnlr
    // Generic Mongo CRUD helper methods requested for direct document operations.
    public void insertDocument(String collectionName, Map<String, Object> documentData) {
        DBConnection.getDatabase().getCollection(collectionName).insertOne(new Document(documentData));
    }

    public List<Document> findDocuments(String collectionName, Map<String, Object> filterData) {
        List<Document> docs = new ArrayList<>();
        Bson filter = new Document(filterData);
        for (Document doc : DBConnection.getDatabase().getCollection(collectionName).find(filter)) {
            docs.add(doc);
        }
        return docs;
    }

    public long updateDocument(String collectionName, Map<String, Object> filterData, Map<String, Object> updates) {
        Bson filter = new Document(filterData);
        Bson updateOp = new Document("$set", new Document(updates));
        UpdateResult result = DBConnection.getDatabase().getCollection(collectionName).updateMany(filter, updateOp);
        return result.getModifiedCount();
    }

    public long deleteDocument(String collectionName, Map<String, Object> filterData) {
        Bson filter = new Document(filterData);
        DeleteResult result = DBConnection.getDatabase().getCollection(collectionName).deleteMany(filter);
        return result.getDeletedCount();
    }
}
