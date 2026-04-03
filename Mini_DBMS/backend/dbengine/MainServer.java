package backend.dbengine;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MainServer {

    public static void main(String[] args) {
        try {
            // Try to initialize once at startup; keep server alive even if DB is unavailable.
            try {
                DBConnection.initialize();
            } catch (IllegalStateException ex) {
                System.err.println("Warning: starting server without DB connection. " + ex.getMessage());
            }

            int requestedPort = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
            int maxPort = requestedPort + 5;
            HttpServer server = null;
            int activePort = requestedPort;

            for (int port = requestedPort; port <= maxPort; port++) {
                try {
                    server = HttpServer.create(new InetSocketAddress(port), 0);
                    activePort = port;
                    break;
                } catch (java.net.BindException ignored) {
                    // Try the next port.
                }
            }

            if (server == null) {
                throw new java.net.BindException("No available port in range " + requestedPort + "-" + maxPort);
            }

            server.createContext("/query", (HttpExchange exchange) -> {

            // CORS fix
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
            exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

            // OPTIONS request handle
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                if (!DBConnection.isReady()) {
                    sendDbUnavailable(exchange);
                    return;
                }
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
                    String query = reader.readLine();
                    System.out.println("Query received: " + query);

                    QueryExecutor executor = new QueryExecutor();
                    String result = executor.executeWeb(query);
                    byte[] response = result.getBytes();

                    exchange.sendResponseHeaders(200, response.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response);
                    }
                }
            }  

        });

        server.createContext("/tables", (HttpExchange exchange) -> {

            // CORS fix
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS");
            exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

            // OPTIONS request handle
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                if (!DBConnection.isReady()) {
                    sendDbUnavailable(exchange);
                    return;
                }
                StorageManager storageManager = new StorageManager();
                List<String> tables = storageManager.getTableNames();
                StringBuilder response = new StringBuilder();
                for (String table : tables) {
                    response.append(table).append("\n");
                }

                byte[] responseBytes = response.toString().getBytes();

                exchange.sendResponseHeaders(200, responseBytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(responseBytes);
                }
            }

        });

        server.createContext("/health", (HttpExchange exchange) -> {
            // CORS fix
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS");
            exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                boolean dbReady = DBConnection.isReady();
                String body = "{\"status\":\"" + (dbReady ? "ok" : "degraded")
                        + "\",\"database\":\"" + (dbReady ? "connected" : "unavailable")
                        + "\",\"message\":\"" + escapeJson(DBConnection.getLastError()) + "\"}";
                byte[] response = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(dbReady ? 200 : 503, response.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
            }
        });

        server.createContext("/upload", (HttpExchange exchange) -> {
            // CORS fix
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "POST, OPTIONS");
            exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                if (!DBConnection.isReady()) {
                    sendDbUnavailable(exchange);
                    return;
                }
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
                    StringBuilder jsonBuilder = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        jsonBuilder.append(line);
                    }
                    String json = jsonBuilder.toString();

                    // Simple JSON parsing (tableName and content)
                    String tableName = json.split("\"tableName\":\"")[1].split("\"")[0];
                    String content = json.split("\"content\":\"")[1].split("\"}")[0].replace("\\n", "\n").replace("\\r", "\r");

                    // Persist uploaded CSV into MongoDB collection instead of local file.
                    String[] lines = content.split("\\n");
                    List<String> nonEmptyLines = new ArrayList<>();
                    for (String lineValue : lines) {
                        if (lineValue != null && !lineValue.trim().isEmpty()) {
                            nonEmptyLines.add(lineValue.trim());
                        }
                    }

                    if (nonEmptyLines.isEmpty()) {
                        exchange.sendResponseHeaders(400, -1);
                        return;
                    }

                    String[] headers = nonEmptyLines.get(0).split(",");
                    for (int i = 0; i < headers.length; i++) {
                        headers[i] = headers[i].trim();
                    }

                    String[] types = new String[headers.length];
                    for (int i = 0; i < types.length; i++) {
                        types[i] = "VARCHAR";
                    }

                    StorageManager storageManager = new StorageManager();
                    storageManager.dropTable(tableName);
                    storageManager.createTable(tableName, headers, types);

                    for (int i = 1; i < nonEmptyLines.size(); i++) {
                        String[] values = nonEmptyLines.get(i).split(",");
                        for (int j = 0; j < values.length; j++) {
                            values[j] = values[j].trim();
                        }
                        storageManager.insertRow(tableName, values);
                    }

                    byte[] response = "OK".getBytes();
                    exchange.sendResponseHeaders(200, response.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    exchange.sendResponseHeaders(500, -1);
                }
            }
        });

        server.createContext("/schema", (HttpExchange exchange) -> {

            // CORS fix
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS");
            exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

            // OPTIONS request handle
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                if (!DBConnection.isReady()) {
                    sendDbUnavailable(exchange);
                    return;
                }
                String query = exchange.getRequestURI().getQuery();
                String tableName = null;
                if (query != null && query.startsWith("table=")) {
                    tableName = query.substring(6);
                }
                if (tableName == null) {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }

                QueryExecutor executor = new QueryExecutor();
                String schema = executor.getTableSchema(tableName);
                byte[] response = schema.getBytes();

                exchange.sendResponseHeaders(200, response.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
            }

        });

        server.createContext("/table-description", (HttpExchange exchange) -> {

            // CORS fix
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS");
            exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                if (!DBConnection.isReady()) {
                    sendDbUnavailable(exchange);
                    return;
                }
                String query = exchange.getRequestURI().getQuery();
                String tableName = null;
                if (query != null && query.startsWith("table=")) {
                    tableName = URLDecoder.decode(query.substring(6), StandardCharsets.UTF_8);
                }
                if (tableName == null || tableName.isBlank()) {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }

                QueryExecutor executor = new QueryExecutor();
                String description = executor.getTableDescription(tableName);

                String apiKey = System.getenv("GEMINI_API_KEY");
                if (apiKey != null && !apiKey.isBlank()) {
                    try {
                        String schema = executor.getTableSchema(tableName);
                        String prompt = "You are describing database tables for students in simple clear English. "
                                + "Write a practical table description with these sections: "
                                + "1) What this table stores (2-3 lines), "
                                + "2) Column meanings (one line per column), "
                                + "3) What one row represents, "
                                + "4) Why this table is useful. "
                                + "Avoid guessing unsupported facts. "
                                + "Table name: " + tableName + "\n"
                                + "Schema:\n" + schema + "\n"
                                + "Fallback description:\n" + description;

                        String aiDescription = generateGeminiText(prompt, apiKey);
                        if (aiDescription != null && !aiDescription.isBlank()) {
                            description = aiDescription;
                        }
                    } catch (Exception ignored) {
                        // Keep deterministic fallback description if AI call fails.
                    }
                }

                byte[] response = description.getBytes(StandardCharsets.UTF_8);

                exchange.sendResponseHeaders(200, response.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
            }

        });

        server.createContext("/chat", (HttpExchange exchange) -> {
            // CORS fix
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "POST, OPTIONS");
            exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
                    StringBuilder msgBuilder = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        msgBuilder.append(line);
                    }
                    String message = msgBuilder.toString().trim();

                    // Get API key from environment variable
                    String apiKey = System.getenv("GEMINI_API_KEY");
                    if (apiKey == null || apiKey.isEmpty()) {
                        String errMsg = "{\"error\": \"API key not configured\"}";
                        byte[] response = errMsg.getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(500, response.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(response);
                        }
                        return;
                    }

                    try {
                        String response = generateGeminiRaw(message, apiKey);
                        if (response == null || response.isBlank()) {
                            response = "{\"error\": \"No response from API\"}";
                        }

                        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, responseBytes.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(responseBytes);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        String errMsg = "{\"error\": \"" + escapeJson(e.toString()) + "\"}";
                        byte[] errResponse = errMsg.getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(500, errResponse.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(errResponse);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    exchange.sendResponseHeaders(400, -1);
                }
            }
        });

        server.start();

        System.out.println("Server running at http://localhost:" + activePort);
        Runtime.getRuntime().addShutdownHook(new Thread(DBConnection::close));

        // Keep the server running
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("Server interrupted");
        }
        } catch (java.net.BindException ex) {
            System.err.println("Error: no free port found for server startup.");
            System.err.println("Set PORT env var or free ports in the configured range.");
            System.exit(1);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    private static String escapeJson(String input) {
        if (input == null) return "";
        return input.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r");
    }

    private static void sendDbUnavailable(HttpExchange exchange) throws IOException {
        String message = "Database unavailable. Set MONGODB_URI or start local MongoDB. Details: "
                + DBConnection.getLastError();
        byte[] response = message.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(503, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }

    private static String generateGeminiText(String prompt, String apiKey) throws IOException {
        String[] models = new String[] {
                "gemini-2.0-flash",
                "gemini-1.5-flash-latest",
                "gemini-1.5-flash"
        };

        for (String model : models) {
            String response = generateGeminiRaw(prompt, apiKey, model);
            if (response == null || response.isBlank()) {
                continue;
            }
            if (response.contains("\"error\"")) {
                continue;
            }

            Pattern textPattern = Pattern.compile("\\\"text\\\"\\s*:\\s*\\\"(.*?)\\\"", Pattern.DOTALL);
            Matcher matcher = textPattern.matcher(response);
            if (matcher.find()) {
                return unescapeJsonString(matcher.group(1)).trim();
            }
        }

        return "";
    }

    private static String generateGeminiRaw(String prompt, String apiKey) throws IOException {
        String[] models = new String[] {
                "gemini-2.0-flash",
                "gemini-1.5-flash-latest",
                "gemini-1.5-flash"
        };

        for (String model : models) {
            String response = generateGeminiRaw(prompt, apiKey, model);
            if (response == null || response.isBlank()) {
                continue;
            }
            if (response.contains("\"error\"")) {
                continue;
            }
            return response;
        }

        return "";
    }

    private static String generateGeminiRaw(String prompt, String apiKey, String model) throws IOException {
        String geminiUrl = "https://generativelanguage.googleapis.com/v1beta/models/" + model + ":generateContent?key=" + apiKey;
        String jsonPayload = "{\"contents\": [{\"role\": \"user\", \"parts\": [{\"text\": \"" + escapeJson(prompt) + "\"}]}]}";

        URL url = new URL(geminiUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        conn.setDoInput(true);

        byte[] payloadBytes = jsonPayload.getBytes(StandardCharsets.UTF_8);
        conn.setFixedLengthStreamingMode(payloadBytes.length);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(payloadBytes);
            os.flush();
        }

        int responseCode = conn.getResponseCode();
        InputStream inputStream;
        if (responseCode >= 200 && responseCode < 300) {
            inputStream = conn.getInputStream();
        } else {
            inputStream = conn.getErrorStream();
            if (inputStream == null) {
                conn.disconnect();
                return "";
            }
        }

        StringBuilder responseBuilder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                responseBuilder.append(line);
            }
        }
        conn.disconnect();

        return responseBuilder.toString();
    }

    private static String unescapeJsonString(String value) {
        if (value == null) return "";
        return value.replace("\\n", "\n")
                    .replace("\\r", "\r")
                    .replace("\\t", "\t")
                    .replace("\\\"", "\"")
                    .replace("\\\\", "\\");
    }
}
