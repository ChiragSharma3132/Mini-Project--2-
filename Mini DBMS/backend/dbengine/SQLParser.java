package backend.dbengine;
public class SQLParser {

    public static String getQueryType(String query) {

        String trimmed = query.trim();

        // Require exact uppercase command keywords (case-sensitive SQL keywords)
        // Only the keyword parts are checked; table/column/value casing is preserved.
        if (trimmed.startsWith("CREATE TABLE")) {
            return "CREATE";
        }

        if (trimmed.startsWith("INSERT INTO")) {
            return "INSERT";
        }

        if (trimmed.startsWith("SELECT")) {
            return "SELECT";
        }

        if (trimmed.startsWith("UPDATE")) {
            return "UPDATE";
        }

        if (trimmed.startsWith("DELETE")) {
            return "DELETE";
        }

        if (trimmed.startsWith("DROP TABLE")) {
            return "DROP";
        }

        return "UNKNOWN";
    }
}