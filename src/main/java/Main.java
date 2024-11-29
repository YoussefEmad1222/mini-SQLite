public class Main {
    public static void main(String[] args) {
        Database database = new Database();
        if (args.length < 2) {
            database.printError("Missing arguments. Usage: java Main <database file> <command>");
            return;
        }

        String databaseFilePath = args[0];
        String command = args[1];
        handleCommand(databaseFilePath, command);
    }

    public static void handleCommand(String databaseFilePath, String command) {
        Database database = new Database();
        switch (command) {
            case ".dbinfo":
                database.getDbInfo(databaseFilePath);
                break;
            case ".tables":
                database.printTablesNames(databaseFilePath);
                break;
            default:
                database.executeQuery(databaseFilePath, command);
        }
    }


}
