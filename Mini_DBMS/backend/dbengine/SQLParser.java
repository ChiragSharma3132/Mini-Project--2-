package backend.dbengine;
public class SQLParser {

    public static String getQueryType(String query) {

        String trimmed = query.trim();
        String upper = trimmed.toUpperCase();

        // Require exact uppercase command keywords (case-sensitive SQL keywords)
        // Only the keyword parts are checked; table/column/value casing is preserved.
        if (upper.startsWith("CREATE TABLE")) {
            return "CREATE";
        }

        if (upper.startsWith("INSERT INTO")) {
            return "INSERT";
        }

        if (upper.startsWith("SELECT")) {
            return "SELECT";
        }

        if (upper.startsWith("UPDATE")) {
            return "UPDATE";
        }

        if (upper.startsWith("DELETE")) {
            return "DELETE";
        }

        if (upper.startsWith("DROP TABLE")) {
            return "DROP";
        }

        return "UNKNOWN";
    }
}