package backend.dbengine;
import java.io.*;
import java.util.*;

public class StorageManager {

    private final String DATA_FOLDER = "data/";

    /**
     * Resolve the CSV file for a given table name.
     *
     * On Windows the filesystem is case-insensitive, so we enforce
     * case-sensitive table-name matching by only returning a file when
     * the exact case matches an existing file.
     */
    private File resolveTableFile(String tableName, boolean forCreation) {
        File folder = new File(DATA_FOLDER);
        if (!folder.exists()) {
            folder.mkdirs();
        }

        String desiredName = tableName + ".csv";
        File[] files = folder.listFiles();
        if (files != null) {
            // Prefer an exact-case match
            for (File f : files) {
                if (f.getName().equals(desiredName)) {
                    return f;
                }
            }
            // For creation, if a file differs only in case, treat it as "exists" to avoid collisions
            if (forCreation) {
                for (File f : files) {
                    if (f.getName().equalsIgnoreCase(desiredName)) {
                        return f;
                    }
                }
            }
        }

        return new File(folder, desiredName);
    }

    public void createTable(String tableName, String[] columns, String[] types) {
        try {
            File file = resolveTableFile(tableName, true);

            if (file.exists()) {
                System.out.println("Table already exists");
                return;
            }

            try (PrintWriter writer = new PrintWriter(file)) {
                for (int i = 0; i < columns.length; i++) {
                    writer.print(columns[i]);
                    if (i != columns.length - 1) writer.print(",");
                }
                writer.println();
            }

            // Store types in a separate file
            File typesFile = new File(DATA_FOLDER + "tables/" + tableName + ".types");
            typesFile.getParentFile().mkdirs();
            try (PrintWriter typeWriter = new PrintWriter(typesFile)) {
                for (int i = 0; i < types.length; i++) {
                    typeWriter.print(types[i]);
                    if (i != types.length - 1) typeWriter.print(",");
                }
                typeWriter.println();
            }
        } catch (IOException e) {
            System.err.println("StorageManager I/O error: " + e.getMessage());
        }
    }

    public void insertRow(String tableName, String[] values) {
        try {
            File file = resolveTableFile(tableName, false);
            if (!file.exists()) {
                System.out.println("Table not found: " + tableName);
                return;
            }

            try (FileWriter writer = new FileWriter(file, true)) {
                for (int i = 0; i < values.length; i++) {
                    writer.append(values[i]);
                    if (i != values.length - 1) writer.append(",");
                }
                writer.append("\n");
            }
        } catch (IOException e) {
            System.err.println("StorageManager I/O error: " + e.getMessage());
        }
    }

    public List<String[]> readTable(String tableName) {
        List<String[]> rows = new ArrayList<>();

        try {
            File file = resolveTableFile(tableName, false);
            if (!file.exists()) {
                System.out.println("Table not found: " + tableName);
                return rows;
            }

            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    rows.add(line.split(","));
                }
            }
        } catch (IOException e) {
            System.err.println("StorageManager I/O error: " + e.getMessage());
        }

        return rows;
    }

    public int updateRows(String tableName, java.util.Map<String, String> updates, String whereColumn, String whereValue) {
        File file = resolveTableFile(tableName, false);
        if (!file.exists()) return 0;

        java.util.List<String[]> rows = readTable(tableName);
        if (rows.isEmpty()) return 0;

        String[] header = rows.get(0);
        java.util.Map<String, Integer> colIndex = new java.util.HashMap<>();
        for (int i = 0; i < header.length; i++) {
            colIndex.put(header[i], i);
        }

        if (!colIndex.containsKey(whereColumn)) return 0;
        int whereIdx = colIndex.get(whereColumn);

        java.util.List<String[]> updatedRows = new java.util.ArrayList<>();
        updatedRows.add(header);

        int updatedCount = 0;
        for (int r = 1; r < rows.size(); r++) {
            String[] row = rows.get(r);
            if (row.length <= whereIdx) {
                updatedRows.add(row);
                continue;
            }
            if (row[whereIdx].equals(whereValue)) {
                for (var entry : updates.entrySet()) {
                    String col = entry.getKey();
                    String val = entry.getValue();
                    Integer idx = colIndex.get(col);
                    if (idx != null && idx < row.length) {
                        row[idx] = val;
                    }
                }
                updatedCount++;
            }
            updatedRows.add(row);
        }

        // Rewrite file
        try (PrintWriter writer = new PrintWriter(new FileWriter(DATA_FOLDER + tableName + ".csv", false))) {
            for (String[] r : updatedRows) {
                for (int i = 0; i < r.length; i++) {
                    writer.print(r[i]);
                    if (i != r.length - 1) writer.print(",");
                }
                writer.println();
            }
        } catch (IOException e) {
            System.err.println("StorageManager I/O error: " + e.getMessage());
        }

        return updatedCount;
    }

    public int deleteRows(String tableName, String whereColumn, String whereValue) {
        File file = resolveTableFile(tableName, false);
        if (!file.exists()) return 0;

        java.util.List<String[]> rows = readTable(tableName);
        if (rows.isEmpty()) return 0;

        String[] header = rows.get(0);
        java.util.Map<String, Integer> colIndex = new java.util.HashMap<>();
        for (int i = 0; i < header.length; i++) {
            colIndex.put(header[i], i);
        }

        if (!colIndex.containsKey(whereColumn)) return 0;
        int whereIdx = colIndex.get(whereColumn);

        java.util.List<String[]> remaining = new java.util.ArrayList<>();
        remaining.add(header);

        int deleteCount = 0;
        for (int r = 1; r < rows.size(); r++) {
            String[] row = rows.get(r);
            if (row.length > whereIdx && row[whereIdx].equals(whereValue)) {
                deleteCount++;
            } else {
                remaining.add(row);
            }
        }

        // Rewrite file
        try (PrintWriter writer = new PrintWriter(new FileWriter(DATA_FOLDER + tableName + ".csv", false))) {
            for (String[] r : remaining) {
                for (int i = 0; i < r.length; i++) {
                    writer.print(r[i]);
                    if (i != r.length - 1) writer.print(",");
                }
                writer.println();
            }
        } catch (IOException e) {
            System.err.println("StorageManager I/O error: " + e.getMessage());
        }

        return deleteCount;
    }

    public void dropTable(String tableName) {
        File file = resolveTableFile(tableName, false);
        if (file.exists()) {
            if (!file.delete()) {
                System.err.println("Failed to delete table file: " + file.getPath());
            }
        }
        // Also delete types file
        File typesFile = new File(DATA_FOLDER + "tables/" + tableName + ".types");
        if (typesFile.exists()) {
            typesFile.delete();
        }
    }

    public String[] getTableTypes(String tableName) {
        File typesFile = new File(DATA_FOLDER + "tables/" + tableName + ".types");
        if (!typesFile.exists()) {
            return new String[0];
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(typesFile))) {
            String line = reader.readLine();
            if (line != null) {
                return line.split(",");
            }
        } catch (IOException e) {
            System.err.println("StorageManager I/O error: " + e.getMessage());
        }
        return new String[0];
    }
}