package backend.dbengine;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.InetSocketAddress;

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
}