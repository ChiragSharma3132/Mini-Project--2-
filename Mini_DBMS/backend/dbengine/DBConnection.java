package backend.dbengine;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class DBConnection {

    // Prefer MONGODB_URI from environment/.env. Local MongoDB is used as fallback.
    private static final String DEFAULT_URI = "mongodb://127.0.0.1:27017";
    private static final String DEFAULT_DB_NAME = "minidb";

    private static MongoClient client;
    private static MongoDatabase database;
    private static String lastError = "Not initialized";

    private DBConnection() {
    }

    public static synchronized void initialize() {
        if (database != null) {
            return;
        }

        String uri = System.getenv("MONGODB_URI");
        if (uri == null || uri.isBlank()) {
            uri = readMongoUriFromDotEnv();
        }
        if (uri == null || uri.isBlank()) {
            uri = DEFAULT_URI;
        }

        String dbName = DEFAULT_DB_NAME;
        ConnectionString connString = new ConnectionString(uri);
        if (connString.getDatabase() != null && !connString.getDatabase().isBlank()) {
            dbName = connString.getDatabase();
        }

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connString)
                .build();

        try {
            client = MongoClients.create(settings);
            database = client.getDatabase(dbName);

            // Validate connectivity once and keep status for endpoint checks.
            database.runCommand(new Document("ping", 1));
            lastError = "";
            System.out.println("MongoDB connected to database: " + dbName);
        } catch (RuntimeException ex) {
            if (client != null) {
                client.close();
                client = null;
            }
            database = null;
            lastError = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
            throw new IllegalStateException("MongoDB connection failed: " + lastError, ex);
        }
    }

    public static MongoDatabase getDatabase() {
        if (database == null) {
            throw new IllegalStateException("MongoDB connection is not initialized. "
                    + "Set MONGODB_URI or start local MongoDB at " + DEFAULT_URI + ". "
                    + "Last error: " + lastError);
        }
        return database;
    }

    public static boolean isReady() {
        return database != null;
    }

    public static String getLastError() {
        return lastError;
    }

    public static synchronized void close() {
        if (client != null) {
            client.close();
            client = null;
            database = null;
            lastError = "Closed";
        }
    }

    private static String readMongoUriFromDotEnv() {
        Path envPath = Path.of(System.getProperty("user.dir"), ".env");
        if (!Files.exists(envPath)) {
            return "";
        }

        try {
            for (String rawLine : Files.readAllLines(envPath, StandardCharsets.UTF_8)) {
                String line = rawLine.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                if (line.startsWith("MONGODB_URI=")) {
                    return line.substring("MONGODB_URI=".length()).trim();
                }
            }
        } catch (IOException ex) {
            lastError = "Failed to read .env: " + ex.getMessage();
        }

        return "";
    }
}
