package backend.dbengine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryExecutor {
    private final StorageManager storageManager = new StorageManager();
    
    public QueryExecutor() {
    }
public String executeWeb(String query) {

    String type = SQLParser.getQueryType(query);

    switch(type) {

        case "CREATE" -> {
            return createTable(query);
            }

        case "INSERT" -> {
            int inserted = insert(query);
            return inserted + " row(s) inserted";
            }

        case "DROP" -> {
            dropTable(query);
            return "Table dropped";
            }

        case "SELECT" -> {
            return selectWeb(query);
            }

        case "UPDATE" -> {
            return update(query);
            }

        case "DELETE" -> {
            return delete(query);
            }


        default -> {
            return "Invalid Query";
            }
    }
}

private String selectWeb(String query) {
    String cleanQuery = query.trim().replaceAll(";$", "");
    String upperQuery = cleanQuery.toUpperCase();

    int fromIndex = upperQuery.indexOf(" FROM ");
    if (fromIndex == -1) {
        return "Invalid SELECT syntax";
    }

    String selectClause = cleanQuery.substring("SELECT".length(), fromIndex).trim();
    if (selectClause.isEmpty()) {
        return "Invalid SELECT syntax";
    }

    int whereIndex = upperQuery.indexOf(" WHERE ", fromIndex);
    int limitIndex = upperQuery.indexOf(" LIMIT ", fromIndex);

    String tableName;
    String whereCol = null;
    String whereVal = null;
    int limit = -1; // -1 means no limit

    if (whereIndex != -1) {
        tableName = cleanQuery.substring(fromIndex + " FROM ".length(), whereIndex).trim();
        int whereEnd = (limitIndex != -1) ? limitIndex : cleanQuery.length();
        String whereClause = cleanQuery.substring(whereIndex + " WHERE ".length(), whereEnd).trim();
        String[] whereParts = whereClause.split("=");
        if (whereParts.length != 2) {
            return "Invalid WHERE syntax";
        }
        whereCol = whereParts[0].trim();
        whereVal = whereParts[1].trim();
    } else if (limitIndex != -1) {
        tableName = cleanQuery.substring(fromIndex + " FROM ".length(), limitIndex).trim();
    } else {
        tableName = cleanQuery.substring(fromIndex + " FROM ".length()).trim();
    }

    if (tableName.isEmpty()) {
        return "Invalid SELECT syntax";
    }

    if (limitIndex != -1) {
        String limitStr = cleanQuery.substring(limitIndex + " LIMIT ".length()).trim();
        try {
            limit = Integer.parseInt(limitStr);
        } catch (NumberFormatException e) {
            return "Invalid LIMIT value";
        }
    }

    // Parse selected expressions (can be columns or arithmetic like col1 + col2)
    String[] selectExprs = selectClause.split(",");
    for (int i = 0; i < selectExprs.length; i++) {
        selectExprs[i] = selectExprs[i].trim();
    }
    boolean selectAll = (selectExprs.length == 1 && "*".equals(selectExprs[0]));

    try {
        java.util.List<String[]> rows = storageManager.readTable(tableName);
        if (rows.isEmpty()) {
            return "(no data)";
        }

        // Build header index mapping (strip datatypes like "INT"/"VARCHAR" etc.)
        String[] rawHeader = rows.get(0);
        String[] normalizedHeader = new String[rawHeader.length];
        java.util.Map<String, Integer> colIndex = new java.util.HashMap<>();
        for (int i = 0; i < rawHeader.length; i++) {
            String h = rawHeader[i];
            String clean = h.split("\\s+")[0];
            normalizedHeader[i] = clean;
            colIndex.put(clean, i);
        }

        // Filter rows based on WHERE
        java.util.List<String[]> filteredRows = new java.util.ArrayList<>();
        filteredRows.add(rows.get(0)); // header placeholder (raw header kept for data alignment)
        if (whereCol != null && whereVal != null) {
            Integer idx = colIndex.get(whereCol);
            if (idx == null) {
                return "Column '" + whereCol + "' not found";
            }
            for (int r = 1; r < rows.size(); r++) {
                String[] row = rows.get(r);
                if (row.length > idx && whereVal.equals(row[idx])) {
                    filteredRows.add(row);
                }
            }
        } else {
            filteredRows.addAll(rows.subList(1, rows.size()));
        }

        // If the query is not selecting all columns, build a synthetic header row
        String[] outputHeader;
        if (selectAll) {
            outputHeader = normalizedHeader;
        } else {
            outputHeader = selectExprs;
        }

        // Apply limit if specified
        int maxDataRows = filteredRows.size() - 1;
        if (limit > 0 && limit < maxDataRows) {
            maxDataRows = limit;
        }

        // Determine column widths
        int columns = outputHeader.length;
        int[] widths = new int[columns];
        for (int i = 0; i < columns; i++) {
            widths[i] = outputHeader[i].length();
        }

        java.util.List<String[]> outputRows = new java.util.ArrayList<>();
        outputRows.add(outputHeader);

        for (int r = 1; r <= maxDataRows; r++) {
            String[] row = filteredRows.get(r);
            String[] outRow = new String[columns];
            for (int c = 0; c < columns; c++) {
                String expr = outputHeader[c];
                if (selectAll) {
                    outRow[c] = (c < row.length) ? row[c] : "";
                } else if (expr.contains("+")) {
                    String[] terms = expr.split("\\+");
                    double sum = 0;
                    boolean anyNumeric = false;
                    for (String term : terms) {
                        String t = term.trim();
                        if (t.isEmpty()) continue;
                        String val;
                        if (colIndex.containsKey(t)) {
                            int idx = colIndex.get(t);
                            val = (idx < row.length) ? row[idx] : "";
                        } else {
                            val = t;
                        }
                        if (val == null || val.isEmpty()) continue;
                        try {
                            sum += Double.parseDouble(val);
                            anyNumeric = true;
                        } catch (NumberFormatException ignored) {
                            // non-numeric term; ignore
                        }
                    }
                    if (anyNumeric) {
                        if (sum == (long) sum) {
                            outRow[c] = String.valueOf((long) sum);
                        } else {
                            outRow[c] = String.valueOf(sum);
                        }
                    } else {
                        outRow[c] = "";
                    }
                } else {
                    Integer idx = colIndex.get(expr);
                    outRow[c] = (idx != null && idx < row.length) ? row[idx] : "";
                }
                widths[c] = Math.max(widths[c], outRow[c] != null ? outRow[c].length() : 0);
            }
            outputRows.add(outRow);
        }

        StringBuilder output = new StringBuilder();

        // Header row
        String[] displayHeader = outputHeader;
        for (int i = 0; i < columns; i++) {
            output.append(pad(displayHeader[i], widths[i]));
            if (i < columns - 1) output.append(" | ");
        }
        output.append("\n");

        // Separator line
        for (int i = 0; i < columns; i++) {
            output.append("-".repeat(widths[i]));
            if (i < columns - 1) output.append("-+-");
        }
        output.append("\n");

        // Data rows
        for (int r = 1; r < outputRows.size(); r++) {
            String[] row = outputRows.get(r);
            for (int c = 0; c < columns; c++) {
                output.append(pad(row[c], widths[c]));
                if (c < columns - 1) output.append(" | ");
            }
            output.append("\n");
        }

        return output.toString();

    } catch (Exception e) {
        return "Error reading table";
    }
}

private String pad(String value, int width) {
    if (value == null) value = "";
    if (value.length() >= width) return value;
    return value + " ".repeat(width - value.length());
}

    private String createTable(String query) {
        try {
            // CREATE TABLE tableName (col1 TYPE, col2 TYPE, ...)
            String trimmed = query.trim();
            String upperQuery = trimmed.toUpperCase();
            int start = "CREATE TABLE ".length();
            int parenStart = upperQuery.indexOf('(', start);
            if (parenStart == -1) {
                return "Invalid CREATE TABLE syntax: missing '('.";
            }
            String tableName = trimmed.substring(start, parenStart).trim();
            int parenEnd = upperQuery.lastIndexOf(')');
            if (parenEnd == -1 || parenEnd < parenStart) {
                return "Invalid CREATE TABLE syntax: missing ')'.";
            }
            String columnsStr = trimmed.substring(parenStart + 1, parenEnd).trim();
            if (columnsStr.isEmpty()) {
                return "Invalid CREATE TABLE syntax: no columns specified.";
            }

            String[] columnDefs = columnsStr.split(",");
            String[] columns = new String[columnDefs.length];
            String[] types = new String[columnDefs.length];

            java.util.Set<String> allowedTypes = new java.util.HashSet<>(java.util.Arrays.asList("INT", "VARCHAR", "FLOAT"));

            for (int i = 0; i < columnDefs.length; i++) {
                String colDef = columnDefs[i].trim();
                if (colDef.isEmpty()) {
                    return "Invalid column definition: empty column.";
                }

                String[] parts = colDef.split("\\s+");
                if (parts.length < 2) {
                    return "Invalid column definition '" + colDef + "'. Expected 'name TYPE'.";
                }

                String colName = parts[0];
                String typeToken = parts[1];

                // Allow optional size specifier, e.g. VARCHAR(255)
                String baseType = typeToken.split("\\(")[0];
                if (!baseType.equals(baseType.toUpperCase())) {
                    return "Data type must be in uppercase (e.g. INT, VARCHAR, FLOAT).";
                }
                if (!allowedTypes.contains(baseType)) {
                    return "Unsupported data type '" + baseType + "'. Allowed: INT, VARCHAR, FLOAT.";
                }

                columns[i] = colName;
                types[i] = baseType;
            }

            storageManager.createTable(tableName, columns, types);
            return "Table Created";
        } catch (Exception e) {
            return "Error creating table: " + e.getMessage();
        }
    }

    private int insert(String query) {
        try {
            // INSERT INTO tableName VALUES (val1, val2, ...), (val1, val2, ...)
            String trimmed = query.trim();
            String upperQuery = trimmed.toUpperCase();
            int intoIndex = upperQuery.indexOf("INTO ") + "INTO ".length();
            int valuesIndex = upperQuery.indexOf(" VALUES ", intoIndex);
            if (valuesIndex == -1) {
                return 0;
            }
            String tableName = trimmed.substring(intoIndex, valuesIndex).trim();

            String valuesPart = trimmed.substring(valuesIndex + " VALUES ".length()).trim();
            java.util.regex.Pattern rowPattern = java.util.regex.Pattern.compile("\\(([^)]*)\\)");
            java.util.regex.Matcher matcher = rowPattern.matcher(valuesPart);

            int insertedCount = 0;
            while (matcher.find()) {
                String valuesStr = matcher.group(1).trim();
                if (valuesStr.isEmpty()) {
                    continue;
                }
                String[] values = valuesStr.split(",");
                for (int i = 0; i < values.length; i++) {
                    values[i] = values[i].trim();
                    // Remove trailing ')' or ';' that may remain from parsing
                    while (values[i].endsWith(")") || values[i].endsWith(";")) {
                        values[i] = values[i].substring(0, values[i].length() - 1).trim();
                    }
                    // Remove quotes if present
                    if (values[i].startsWith("'") && values[i].endsWith("'")) {
                        values[i] = values[i].substring(1, values[i].length() - 1);
                    }
                }
                if (storageManager.insertRow(tableName, values)) {
                    insertedCount++;
                }
            }

            return insertedCount;
        } catch (Exception e) {
            System.out.println("Error inserting row: " + e.getMessage());
            return 0;
        }
    }

    private String update(String query) {
        try {
            // UPDATE tableName SET col1=value1, col2=value2 WHERE col=value
            String cleanQuery = query.trim().replaceAll(";+$", "");
            String upperQuery = cleanQuery.toUpperCase();
            int updateIndex = "UPDATE ".length();
            int setIndex = upperQuery.indexOf(" SET ", updateIndex);
            String tableName = cleanQuery.substring(updateIndex, setIndex).trim();

            int whereIndex = upperQuery.indexOf(" WHERE ", setIndex);
            String setClause = cleanQuery.substring(setIndex + " SET ".length(), whereIndex).trim();
            String whereClause = cleanQuery.substring(whereIndex + " WHERE ".length()).trim();

            java.util.Map<String, String> updates = new java.util.HashMap<>();
            for (String part : setClause.split(",")) {
                String[] kv = part.split("=", 2);
                if (kv.length == 2) {
                    updates.put(kv[0].trim(), kv[1].trim());
                }
            }

            String[] whereParts = whereClause.split("=");
            if (whereParts.length != 2) {
                return "syntax error";
            }
            String whereCol = whereParts[0].trim();
            String whereVal = whereParts[1].trim();

            int updated = storageManager.updateRows(tableName, updates, whereCol, whereVal);
            return updated + " row(s) updated";
        } catch (Exception e) {
            return "syntax error";
        }
    }

    private String delete(String query) {
        try {
            // DELETE FROM tableName WHERE col=value
            String cleanQuery = query.trim().replaceAll(";+$", "");
            String upperQuery = cleanQuery.toUpperCase();
            int fromIndex = upperQuery.indexOf("FROM ") + "FROM ".length();
            int whereIndex = upperQuery.indexOf(" WHERE ", fromIndex);
            String tableName = cleanQuery.substring(fromIndex, whereIndex).trim();

            String whereClause = cleanQuery.substring(whereIndex + " WHERE ".length()).trim();
            String[] whereParts = whereClause.split("=", 2);
            if (whereParts.length != 2) {
                return "syntax error";
            }
            String whereCol = whereParts[0].trim();
            String whereVal = whereParts[1].trim();

            int deleted = storageManager.deleteRows(tableName, whereCol, whereVal);
            return deleted + " row(s) deleted";
        } catch (Exception e) {
            return "syntax error";
        }
    }

    private void dropTable(String query) {
        try {
            // DROP TABLE tableName
            String clean = query.trim().replaceAll(";+$", "");
            int dropIndex = "DROP TABLE ".length();
            String tableName = clean.substring(dropIndex).trim();
            storageManager.dropTable(tableName);
        } catch (Exception e) {
            System.out.println("Error dropping table: " + e.getMessage());
        }
    }

    public String getTableSchema(String tableName) {
        try {
            String[] columns = storageManager.readTable(tableName).get(0);
            String[] types = storageManager.getTableTypes(tableName);
            StringBuilder sb = new StringBuilder();
            sb.append("Table: ").append(tableName).append("\n");
            sb.append("Columns:\n");
            for (int i = 0; i < columns.length; i++) {
                String type = (i < types.length) ? types[i] : "UNKNOWN";
                sb.append("- ").append(columns[i]).append(" (").append(type).append(")\n");
            }
            return sb.toString();
        } catch (Exception e) {
            return "Error getting schema";
        }
    }

    public String getTableDescription(String tableName) {
        List<String[]> rows = storageManager.readTable(tableName);
        if (rows.isEmpty()) {
            return "Table '" + tableName + "' was not found.";
        }

        String[] columns = rows.get(0);
        String[] types = storageManager.getTableTypes(tableName);
        int rowCount = Math.max(0, rows.size() - 1);
        int sampleLimit = Math.min(3, rowCount);

        String entity = tableName;
        if (entity.endsWith("s") && entity.length() > 1) {
            entity = entity.substring(0, entity.length() - 1);
        }

        String domain = inferDomain(tableName, columns);

        StringBuilder sb = new StringBuilder();
        sb.append("There is a table named \"").append(tableName).append("\" which stores information about ")
          .append(domain).append(".\n\n");

        sb.append("This table has several columns such as:\n\n");
        for (int i = 0; i < columns.length; i++) {
            String col = columns[i].trim();
            String type = (i < types.length) ? types[i] : "UNKNOWN";
            String sampleValue = pickSampleValue(rows, i);
            sb.append(col)
              .append(" - ")
              .append(explainColumnPurpose(col, entity, type, sampleValue))
              .append(" (type: ")
              .append(type)
              .append(")\n");
        }

        sb.append("\nEach row in the table represents one complete ")
          .append(domain)
          .append(" record.\n");
        sb.append("Current row count: ").append(rowCount).append(".\n\n");

        if (sampleLimit > 0) {
            sb.append("Sample data insight: The first ").append(sampleLimit)
              .append(" row(s) show how records are stored with real values.\n\n");
        }

        sb.append("The table is used to store and organize ")
          .append(domain)
          .append(" data so that it can be accessed, read, and compared easily.");

        return sb.toString();
    }

    private String explainColumnPurpose(String columnName, String entity, String type, String sampleValue) {
        java.util.Set<String> tokens = splitTokens(columnName);
        String subject = entity.toLowerCase();

        if (hasAny(tokens, "id") || endsWithId(columnName)) {
            return "stores the unique identifier for each " + subject;
        }
        if (hasAny(tokens, "name", "firstname", "lastname", "fullname")) {
            return "stores the name details";
        }
        if (hasAny(tokens, "age", "dob", "birthdate")) {
            return "stores age-related information";
        }
        if (hasAny(tokens, "class", "grade", "section", "semester", "year")) {
            return "stores class, grade, or academic grouping details";
        }
        if (hasAny(tokens, "city", "state", "country", "address", "location", "pincode", "zipcode")) {
            return "stores location or address information";
        }
        if (hasAny(tokens, "phone", "mobile", "contact")) {
            return "stores contact number information";
        }
        if (hasAny(tokens, "email")) {
            return "stores email information";
        }
        if (hasAny(tokens, "order", "orderid")) {
            return "stores order-related information";
        }
        if (hasAny(tokens, "customer", "client", "user")) {
            return "stores customer or user-related information";
        }
        if (hasAny(tokens, "restaurant", "vendor", "store")) {
            return "stores seller or provider details";
        }
        if (hasAny(tokens, "rider", "driver", "delivery", "courier")) {
            return "stores delivery agent or delivery process details";
        }
        if (hasAny(tokens, "status", "state")) {
            return "stores the current status of the record";
        }
        if (hasAny(tokens, "date", "time", "timestamp", "created", "updated")) {
            return "stores date and time information";
        }
        if (hasAny(tokens, "amount", "price", "cost", "fee", "salary", "spend", "payment", "total", "subtotal", "bill")) {
            return "stores monetary values or spending details";
        }
        if (hasAny(tokens, "qty", "quantity", "count", "stock")) {
            return "stores quantity or count information";
        }
        if (hasAny(tokens, "rating", "score", "review")) {
            return "stores feedback or rating information";
        }

        if (sampleValue != null && !sampleValue.isBlank()) {
            if (sampleValue.matches("^-?\\d+$")) {
                return "stores numeric values (example: " + sampleValue + ")";
            }
            if (sampleValue.matches("^-?\\d+(\\.\\d+)?$")) {
                return "stores decimal numeric values (example: " + sampleValue + ")";
            }
        }

        if ("INT".equalsIgnoreCase(type) || "FLOAT".equalsIgnoreCase(type)) {
            return "stores measured numeric information";
        }

        return "stores " + columnName + " details";
    }

    private String pickSampleValue(List<String[]> rows, int columnIndex) {
        for (int i = 1; i < rows.size(); i++) {
            String[] row = rows.get(i);
            if (columnIndex < row.length) {
                String value = row[columnIndex] == null ? "" : row[columnIndex].trim();
                if (!value.isEmpty()) {
                    return value;
                }
            }
        }
        return "";
    }

    private String inferDomain(String tableName, String[] columns) {
        java.util.Set<String> allTokens = new java.util.HashSet<>(splitTokens(tableName));
        for (String c : columns) {
            allTokens.addAll(splitTokens(c));
        }

        if (hasAny(allTokens, "student", "class", "grade", "school", "course")) return "students and academic records";
        if (hasAny(allTokens, "food", "order", "delivery", "restaurant", "rider", "menu")) return "food delivery orders and operations";
        if (hasAny(allTokens, "employee", "salary", "department", "hr")) return "employees and organizational records";
        if (hasAny(allTokens, "customer", "client", "purchase", "payment")) return "customers and transactional records";

        String lower = tableName.toLowerCase();
        return lower.endsWith("s") && lower.length() > 1 ? lower : (lower + " records");
    }

    private java.util.Set<String> splitTokens(String value) {
        String spaced = value
            .replaceAll("([a-z])([A-Z])", "$1 $2")
            .replace('_', ' ')
            .replace('-', ' ')
            .toLowerCase();

        java.util.Set<String> tokens = new java.util.HashSet<>();
        for (String part : spaced.split("\\s+")) {
            if (!part.isBlank()) {
                tokens.add(part);
            }
        }
        return tokens;
    }

    private boolean hasAny(java.util.Set<String> tokens, String... candidates) {
        for (String candidate : candidates) {
            if (tokens.contains(candidate)) return true;
        }
        return false;
    }

    private boolean endsWithId(String columnName) {
        String c = columnName.toLowerCase();
        return c.endsWith("id") && !c.equals("grid") && !c.equals("paid");
    }

    public Map<String, Integer> analyze(String tableName, String columnName) {
        List<String[]> rows = storageManager.readTable(tableName);

        Map<String, Integer> freq = new HashMap<>();

        if (rows.isEmpty()) return freq;

        String[] header = rows.get(0);
        int colIndex = -1;

        for (int i = 0; i < header.length; i++) {
            if (header[i].equals(columnName)) {
                colIndex = i;
                break;
            }
        }

        if (colIndex == -1) return freq;

        for (int i = 1; i < rows.size(); i++) {
            String value = rows.get(i)[colIndex];

            freq.put(value, freq.getOrDefault(value, 0) + 1);
        }

        return freq;
    }

    public static String execute(String query) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'execute'");
    }
}
