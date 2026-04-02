package backend.dbengine;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MainServer {

    public static void main(String[] args) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);

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

                File dataFolder = new File("data");
                String[] files = dataFolder.list((dir, name) -> name.endsWith(".csv"));
                StringBuilder response = new StringBuilder();
                if (files != null) {
                    for (String file : files) {
                        response.append(file.replace(".csv", "")).append("\n");
                    }
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
                byte[] response = "OK".getBytes();
                exchange.sendResponseHeaders(200, response.length);
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

                    // Save to file
                    File dataFolder = new File("data");
                    if (!dataFolder.exists()) dataFolder.mkdirs();
                    try (FileWriter writer = new FileWriter(new File(dataFolder, tableName + ".csv"))) {
                        writer.write(content);
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

                    // Call Gemini API
                    String geminiUrl = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=" + apiKey;
                    String jsonPayload = "{\"contents\": [{\"role\": \"user\", \"parts\": [{\"text\": \"" + escapeJson(message) + "\"}]}]}";

                    try {
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
                        StringBuilder responseBuilder = new StringBuilder();

                        InputStream inputStream;
                        if (responseCode >= 200 && responseCode < 300) {
                            inputStream = conn.getInputStream();
                        } else {
                            inputStream = conn.getErrorStream();
                        }

                        try (BufferedReader apiReader = new BufferedReader(new InputStreamReader(inputStream))) {
                            String apiLine;
                            while ((apiLine = apiReader.readLine()) != null) {
                                responseBuilder.append(apiLine);
                            }
                        }

                        String response = responseBuilder.toString();
                        if (response.length() == 0) {
                            response = "{\"error\": \"No response from API\"}";
                        }

                        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, responseBytes.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(responseBytes);
                        }

                        conn.disconnect();
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

        System.out.println("Server running at http://localhost:8080");

        // Keep the server running
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("Server interrupted");
        }
        } catch (java.net.BindException ex) {
            System.err.println("Error: port 8080 is already in use. Please stop any other process using this port and try again.");
            System.err.println("You can run: netstat -ano | findstr :8080  and then taskkill /PID <pid> /F");
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

    private static String generateGeminiText(String prompt, String apiKey) throws IOException {
        String geminiUrl = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=" + apiKey;
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
        if (responseCode < 200 || responseCode >= 300) {
            conn.disconnect();
            return "";
        }

        StringBuilder responseBuilder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                responseBuilder.append(line);
            }
        }
        conn.disconnect();

        String response = responseBuilder.toString();
        Pattern textPattern = Pattern.compile("\\\"text\\\"\\s*:\\s*\\\"(.*?)\\\"", Pattern.DOTALL);
        Matcher matcher = textPattern.matcher(response);
        if (matcher.find()) {
            return unescapeJsonString(matcher.group(1)).trim();
        }
        return "";
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
