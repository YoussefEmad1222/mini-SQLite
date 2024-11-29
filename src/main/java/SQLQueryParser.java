import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLQueryParser {

    List<String> columns;
    String commandType;
    List<String> tableNames;
    String whereClause;
    String orderByClause;
    String groupByClause;
    String limitClause;
    String offsetClause;
    String[] tableColumns;
    // get the Primary key name from the table
    String primaryKey;

    public SQLQueryParser() {
        columns = new ArrayList<>();
        whereClause = "";
        orderByClause = "";
        groupByClause = "";
        limitClause = "";
        offsetClause = "";
    }

    public boolean parse(String query) {
        // Regex for parsing SELECT queries, including WHERE, ORDER BY, GROUP BY, LIMIT, and OFFSET
        String selectRegex = "(?i)SELECT\\s+(.+)\\s+FROM\\s+([a-zA-Z0-9_]+)\\s*" + "(WHERE\\s+(.+?))?\\s*" + "(ORDER\\s+BY\\s+(.+?))?\\s*" + "(GROUP\\s+BY\\s+(.+?))?\\s*" + "(LIMIT\\s+(\\d+))?\\s*" + "(OFFSET\\s+(\\d+))?;?";

        Pattern pattern = Pattern.compile(selectRegex);
        Matcher matcher = pattern.matcher(query);

        if (matcher.matches()) {
            // Extract command type
            commandType = "SELECT";

            // Extract columns
            String columnPart = matcher.group(1).trim();
            columns.addAll(parseColumns(columnPart));

            // Extract table Names
            tableNames = parseColumns(matcher.group(2).trim());
            // Optional WHERE clause
            if (matcher.group(4) != null) {
                whereClause = matcher.group(4).trim();
            }

            // Optional ORDER BY clause
            if (matcher.group(6) != null) {
                orderByClause = matcher.group(6).trim();
            }

            // Optional GROUP BY clause
            if (matcher.group(8) != null) {
                groupByClause = matcher.group(8).trim();
            }

            // Optional LIMIT clause
            if (matcher.group(10) != null) {
                limitClause = matcher.group(10).trim();
            }

            // Optional OFFSET clause
            if (matcher.group(12) != null) {
                offsetClause = matcher.group(12).trim();
            }

            return true;  // Successfully parsed the query
        }

        return false;  // Failed to parse
    }

    private List<String> parseColumns(String columnPart) {
        List<String> columns = new ArrayList<>();
        String[] parts = columnPart.split(",");
        for (String part : parts) {
            columns.add(part.trim());
        }
        return columns;
    }

    public void printParsedQuery() {
        System.out.println("Command Type: " + commandType);
        System.out.println("Columns: " + columns);
        System.out.println("Table Names: " + tableNames);
        System.out.println("Where Clause: " + whereClause);
        System.out.println("Order By Clause: " + orderByClause);
        System.out.println("Group By Clause: " + groupByClause);
        System.out.println("Limit Clause: " + limitClause);
        System.out.println("Offset Clause: " + offsetClause);
    }


    public void parseTableColumns(Map<String, byte[]> tableInfoOfRootPage) {
        // Get the SQL string from the byte array
        byte[] sql = tableInfoOfRootPage.get("sql");
        String sqlString = new String(sql, StandardCharsets.UTF_8);

        // Define a regex pattern to extract columns from the CREATE TABLE statement
        String columnRegex = "\\((.*?)\\)";
        Pattern pattern = Pattern.compile(columnRegex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sqlString);

        if (matcher.find()) {
            // Extract the content inside parentheses (column definitions)
            String columnsString = matcher.group(1).trim();

            // Use regex to split columns, taking care of commas between column definitions
            String[] columnDefinitions = columnsString.split(",\\s*(?![^()]*\\))"); // avoid splitting on commas inside parentheses

            // Extract just the column names from each definition and check for PRIMARY KEY
            String[] tableColumnsArray = new String[columnDefinitions.length];
            for (int i = 0; i < columnDefinitions.length; i++) {
                // Match the column name (first word) before any spaces
                String columnNameRegex = "^\\s*([a-zA-Z0-9_]+)";
                Pattern columnNamePattern = Pattern.compile(columnNameRegex);
                Matcher columnNameMatcher = columnNamePattern.matcher(columnDefinitions[i]);

                if (columnNameMatcher.find()) {
                    String columnName = columnNameMatcher.group(1);  // Get the column name
                    tableColumnsArray[i] = columnName;

                    // Check if this column definition contains "PRIMARY KEY"
                    if (columnDefinitions[i].toUpperCase().contains("PRIMARY KEY")) {
                        primaryKey = columnName;  // Set this column as the primary key
                    }
                }
            }

            tableColumns = tableColumnsArray;
        }
    }
}
