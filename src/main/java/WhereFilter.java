import java.util.List;
import java.util.Map;


public class WhereFilter {

    private static Condition getCondition(String whereClause) {
        String column = "";
        String operator = "";
        String value = "";
        for (String op : new String[]{"<=", ">=", "=", "<", ">", "<>"}) {
            if (whereClause.contains(op)) {
                String[] parts = whereClause.split(op);
                column = parts[0].trim();
                operator = op;
                value = parts[1].trim();
                if (value.charAt(0) == '\'') {
                    value = value.substring(1, value.length() - 1);
                } else {
                    value = parts[1].trim();
                }
                break;
            }
        }
        if (column.isEmpty() || operator.isEmpty() || value.isEmpty()) {
            throw new IllegalArgumentException("Invalid where clause: " + whereClause);
        }
        return new Condition(column, operator, value);
    }

    public void filter(String whereClause, String[] tableColumnsArray, List<Map<String, byte[]>> rows) {
        if (whereClause == null || whereClause.isEmpty() || rows.isEmpty()) {
            return;
        }

        // Step 1: Parse the whereClause into a binary tree
        Node root = parseWhereClause(whereClause);

        // Step 2: Filter rows based on the whereClause
        for (int i = 0; i < rows.size(); i++) {
            Map<String, byte[]> row = rows.get(i);
            if (!evaluate(root, row, tableColumnsArray)) {
                rows.remove(i);
                i--;
            }

        }
    }

    private boolean evaluate(Node node, Map<String, byte[]> row, String[] tableColumnsArray) {
        if (node == null) {
            return true;
        }
        return switch (node.type) {
            case AND -> evaluate(node.left, row, tableColumnsArray) && evaluate(node.right, row, tableColumnsArray);
            case OR -> evaluate(node.left, row, tableColumnsArray) || evaluate(node.right, row, tableColumnsArray);
            case CONDITION -> evaluateCondition(node.condition, row, tableColumnsArray);
        };
    }

    private boolean evaluateCondition(Condition condition, Map<String, byte[]> row, String[] tableColumnsArray) {
        String column = condition.column;
        String operator = condition.operator;
        String value = condition.value;

        byte[] columnValue = row.get(column);
        if (columnValue == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        // Convert the byte[] value to a string
        String columnValueStr = new String(columnValue);

        // Get the index of the column in the tableColumnsArray
        int columnIndex = -1;
        for (int i = 0; i < tableColumnsArray.length; i++) {
            if (tableColumnsArray[i].equals(column)) {
                columnIndex = i;
                break;
            }
        }

        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column not found in table columns: " + column);
        }

        // Compare the column value with the condition value based on the operator
        return switch (operator) {
            case "<=" -> columnValueStr.compareTo(value) <= 0;
            case ">=" -> columnValueStr.compareTo(value) >= 0;
            case "!=", "<>" -> !columnValueStr.equals(value);
            case "=" -> columnValueStr.equals(value);
            case "<" -> columnValueStr.compareTo(value) < 0;
            case ">" -> columnValueStr.compareTo(value) > 0;
            default -> throw new IllegalArgumentException("Invalid operator: " + operator);
        };
    }

    private Node parseWhereClause(String whereClause) {
        // Remove leading and trailing whitespaces
        whereClause = whereClause.trim();

        if (whereClause.isEmpty()) {
            return null;
        }
        if (whereClause.contains(" OR ")) {
            Node node = new Node(NodeType.OR);
            String[] parts = whereClause.split(" OR ", 1);
            node.left = parseWhereClause(parts[0]);
            node.right = parseWhereClause(parts[1]);
            return node;
        }
        if (whereClause.contains(" AND ")) {
            Node node = new Node(NodeType.AND);
            String[] parts = whereClause.split(" AND ", 1);
            node.left = parseWhereClause(parts[0]);
            node.right = parseWhereClause(parts[1]);
            return node;
        }
        Condition condition = getCondition(whereClause);
        return new Node(NodeType.CONDITION, condition);
    }

    // Enum for representing the type of node in the tree
    enum NodeType {AND, OR, CONDITION}

    // Binary tree node structure
    static class Node {
        NodeType type;  // AND, OR, or CONDITION
        Condition condition;  // Only used for CONDITION nodes
        Node left;  // Left child
        Node right; // Right child

        Node(NodeType type) {
            this.type = type;
        }

        Node(NodeType nodeType, Condition condition) {
            this.type = nodeType;
            this.condition = condition;
        }
    }

    static class Condition {
        String column;
        String operator;
        String value;

        Condition(String column, String operator, String value) {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }
    }

    // Step 1: Parse the whereClause into a binary tree
}
