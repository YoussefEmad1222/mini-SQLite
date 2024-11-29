import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Database {
    private static final int SQLITE_HEADER_SIZE = 100;
    private VarInt ROWID = new VarInt(0, 0);

    public static int byteArrayToInt(byte[] bytes) {
        if (bytes.length > 4) {
            throw new IllegalArgumentException("Byte array too long to convert to int");
        }
        byte[] paddedBytes = new byte[4];
        System.arraycopy(bytes, 0, paddedBytes, 4 - bytes.length, bytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(paddedBytes);
        return buffer.getInt();
    }

    public void printError(String message) {
        System.out.println("Error: " + message);
    }

    private int[] getCellPointers(int numberOfCells, RandomAccessFile dbFile) throws IOException {
        int[] cellPointers = new int[numberOfCells];
        for (int i = 0; i < numberOfCells; i++) {
            cellPointers[i] = Short.toUnsignedInt(dbFile.readShort());
        }
        return cellPointers;
    }

    private boolean isInteriorTable(byte bTreePageType) {
        return bTreePageType == 0x05;
    }

    private boolean isLeafTable(byte bTreePageType) {
        return bTreePageType == 0x0D;
    }

    private int readPageSize(RandomAccessFile dbFile) throws IOException {
        dbFile.seek(16);
        return Short.toUnsignedInt(dbFile.readShort());
    }

    public void getDbInfo(String databaseFilePath) {
        try (RandomAccessFile dbFile = new RandomAccessFile(databaseFilePath, "r")) {
            int pageSize = readPageSize(dbFile);
            System.out.println("database page size: " + pageSize);
            int numberOfTables = getNumberOfTables(dbFile, pageSize, 1);
            System.out.println("number of tables: " + numberOfTables);
        } catch (IOException e) {
            printError("Error reading file: " + e.getMessage());
        }
    }

    private byte getBTreePageType(RandomAccessFile dbFile, int pageSize, int pageNumber) throws IOException {
        long startOfPage = (long) (pageNumber - 1) * pageSize;
        dbFile.seek(startOfPage);
        if (pageNumber == 1) {
            dbFile.skipBytes(SQLITE_HEADER_SIZE);  // Skip SQLite header
        }
        return dbFile.readByte();
    }

    private int getNumberOfCells(RandomAccessFile dbFile) throws IOException {
        dbFile.skipBytes(2);
        int numberOfCells = Short.toUnsignedInt(dbFile.readShort());
        dbFile.skipBytes(3);
        return numberOfCells;
    }

    private int getNumberOfTables(RandomAccessFile dbFile, int pageSize, int pageNumber) throws IOException {
        long originalPosition = dbFile.getFilePointer();
        byte bTreePageType = getBTreePageType(dbFile, pageSize, pageNumber);
        int numberOfCells = getNumberOfCells(dbFile);
        int totalTables = 0;
        if (isInteriorTable(bTreePageType)) {
            totalTables += handleInteriorTable(dbFile, pageSize, pageNumber, numberOfCells);
        } else if (isLeafTable(bTreePageType)) {
            totalTables += numberOfCells;
        }
        dbFile.seek(originalPosition);
        return totalTables;
    }

    private int handleInteriorTable(RandomAccessFile dbFile, int pageSize, int pageNumber, int numberOfCells) throws IOException {
        int totalTables = 0;
        long startOfPage = (long) (pageNumber - 1) * pageSize;
        int rightMostPointer = dbFile.readInt();
        int[] cellPointers = getCellPointers(numberOfCells, dbFile);
        for (int i = 0; i < numberOfCells; i++) {
            dbFile.seek(startOfPage + cellPointers[i]);
            int childPageNumber = dbFile.readInt();
            totalTables += getNumberOfTables(dbFile, pageSize, childPageNumber);
            VarInt.readVarInt(dbFile); // Skipping the VarInt after processing child page
        }
        totalTables += getNumberOfTables(dbFile, pageSize, rightMostPointer);
        return totalTables;
    }

    public void printTablesNames(String dbFilePath) {
        try (RandomAccessFile dbFile = new RandomAccessFile(dbFilePath, "r")) {
            int pageSize = readPageSize(dbFile);
            StringBuilder tableNames = new StringBuilder();
            printTableNames(dbFile, pageSize, 1, tableNames);
            System.out.print("Table names: " + tableNames);
        } catch (IOException e) {
            printError("Error reading file: " + e.getMessage());
        }
    }

    private void handleInteriorTableNames(RandomAccessFile dbFile, int pageSize, int numberOfCells, StringBuilder tableNames, int pageNumber) throws IOException {
        long startOfPage = (long) (pageNumber - 1) * pageSize;
        int rightMostPointer = dbFile.readInt();
        int[] cellPointers = getCellPointers(numberOfCells, dbFile);
        for (int i = 0; i < numberOfCells; i++) {
            dbFile.seek(startOfPage + cellPointers[i]);
            int childPageNumber = dbFile.readInt();
            printTableNames(dbFile, pageSize, childPageNumber, tableNames);
            VarInt.readVarInt(dbFile); // Skipping the VarInt after processing child page
        }
        printTableNames(dbFile, pageSize, rightMostPointer, tableNames);
    }

    private void handleLeafTableNames(RandomAccessFile dbFile, int pageSize, int numberOfCells, StringBuilder tableNames, int pageNumber) throws IOException {
        int[] cellPointers = getCellPointers(numberOfCells, dbFile);
        long startOfPage = (long) (pageNumber - 1) * pageSize;
        for (int i = 0; i < numberOfCells; i++) {
            long seekValue = startOfPage + cellPointers[i];
            dbFile.seek(seekValue);
            Map<String, byte[]> cellData = getCellData(dbFile);
            String tableName = new String(cellData.get("tbl_name"), StandardCharsets.UTF_8);
            tableNames.append(tableName).append(" ");
        }
    }

    private Map<String, byte[]> getCellData(RandomAccessFile dbFile) throws IOException {
        List<VarInt> serialTypes = getSerialTypes(dbFile);
        List<byte[]> values = getSerializedValues(dbFile, serialTypes, dbFile.getFilePointer());
        Map<String, byte[]> cellData = new HashMap<>();
        cellData.put("type", values.get(0));
        cellData.put("name", values.get(1));
        cellData.put("tbl_name", values.get(2));
        cellData.put("rootPage", values.get(3));
        cellData.put("sql", values.get(4));
        return cellData;
    }

    private List<VarInt> getSerialTypes(RandomAccessFile dbFile) throws IOException {
        VarInt recordSize = VarInt.readVarInt(dbFile);
        ROWID = VarInt.readVarInt(dbFile);
        VarInt totalHeaderSize = VarInt.readVarInt(dbFile);
        List<VarInt> serialTypes = new ArrayList<>();
        long headerEnd = dbFile.getFilePointer() + totalHeaderSize.value;
        long dbFilePointer = dbFile.getFilePointer();
        while (dbFilePointer < headerEnd) {
            serialTypes.add(VarInt.readVarInt(dbFile));
            dbFilePointer = dbFile.getFilePointer();
        }
        return serialTypes;
    }

    private List<byte[]> getSerializedValues(RandomAccessFile dbFile, List<VarInt> serialTypes, long headerEnd) throws IOException {
        List<byte[]> values = new ArrayList<>();
        dbFile.seek(headerEnd - 1);
        for (VarInt serialType : serialTypes) {
            long size = getSerialTypeSize(serialType.value);
            byte[] value = new byte[(int) size];
            dbFile.read(value);
            values.add(value);
        }
        return values;
    }

    private void printTableNames(RandomAccessFile dbFile, int pageSize, int pageNumber, StringBuilder tableNames) {
        try {
            long originalPosition = dbFile.getFilePointer();
            byte bTreePageType = getBTreePageType(dbFile, pageSize, pageNumber);
            int numberOfCells = getNumberOfCells(dbFile);
            if (isInteriorTable(bTreePageType)) {
                handleInteriorTableNames(dbFile, pageSize, numberOfCells, tableNames, pageNumber);
            } else if (isLeafTable(bTreePageType)) {
                handleLeafTableNames(dbFile, pageSize, numberOfCells, tableNames, pageNumber);
            }
            dbFile.seek(originalPosition);
        } catch (IOException e) {
            printError("Error reading file: " + e.getMessage());
        }
    }

    private long getSerialTypeSize(long serialTypeCode) {
        if (serialTypeCode >= 0 && serialTypeCode <= 4) {
            return serialTypeCode;
        } else if (serialTypeCode == 5) {
            return 6;
        } else if (serialTypeCode == 6 || serialTypeCode == 7) {
            return 8;
        } else if (serialTypeCode >= 13 && serialTypeCode % 2 == 1) {
            return (serialTypeCode - 13) / 2;  // Text type size
        } else if (serialTypeCode >= 12) {
            return (serialTypeCode - 12) / 2;  // Blob type size
        } else {
            return 0;
        }
    }


    public void executeQuery(String databaseFilePath, String command) {
        String[] commandParts = command.split(" ");
        if (!commandParts[0].equalsIgnoreCase("SELECT")) {
            printError("Invalid command");
            return;
        }

        int fromIndex = -1;
        for (int i = 0; i < commandParts.length; i++) {
            if (commandParts[i].trim().equalsIgnoreCase("FROM")) {
                fromIndex = i;
            }
        }
        if (fromIndex == -1 || fromIndex == commandParts.length - 1) {
            printError("Invalid command");
            return;
        }
        String tableName = commandParts[fromIndex + 1];
        try (RandomAccessFile dbFile = new RandomAccessFile(databaseFilePath, "r")) {
            int pageSize = readPageSize(dbFile);
            Map<String, byte[]> tableInfoOfRootPage = findTableRootPage(dbFile, pageSize, tableName, 1);
            if (tableInfoOfRootPage.isEmpty()) {
                printError("Table " + tableName + " does not exist");
                return;
            }
            SQLQueryParser parser = new SQLQueryParser();
            parser.parse(command);
            parser.parseTableColumns(tableInfoOfRootPage);
            parseColumnsOfCommand(dbFile, pageSize, tableInfoOfRootPage, parser);
        } catch (IOException e) {
            printError("Error reading file: " + e.getMessage());
        }
    }

    private boolean isColumnsExist(List<String> columns, String[] tableColumnsArray) {
        int[] columnsIndex = new int[columns.size()];
        for (int j = 0; j < columns.size(); j++) {
            columnsIndex[j] = getTableColumnIndex(columns.get(j), tableColumnsArray);
            if (columnsIndex[j] == -1) {
                printError("Column " + columns.get(j) + " does not exist");
                return false;
            }
        }
        return true;
    }

    private void parseColumnsOfCommand(RandomAccessFile dbFile, int pageSize, Map<String, byte[]> tableInfoOfRootPage, SQLQueryParser parser) throws IOException {
        List<String> columns = parser.columns;
        String[] tableColumnsArray = parser.tableColumns;
        int rootPage = byteArrayToInt(tableInfoOfRootPage.get("rootPage"));
        List<Map<String, byte[]>> rows = getRows(dbFile, pageSize, rootPage, tableColumnsArray, parser);
        // Handle COUNT(*) separately
        if (columns.size() == 1 && columns.getFirst().equalsIgnoreCase("COUNT(*)")) {
            System.out.println(rows.size());
            return;
        }

        if (!isColumnsExist(columns, tableColumnsArray)) {
            return;
        }
        whereClauseFilter(parser.whereClause, tableColumnsArray, rows);
        for (Map<String, byte[]> row : rows) {
            for (int i = 0; i < columns.size(); i++) {
                byte[] value = row.get(columns.get(i));
                System.out.print(new String(value, StandardCharsets.UTF_8));
                if (i != columns.size() - 1) {
                    System.out.print("|");
                }
            }
            System.out.println();
        }
    }

    private void whereClauseFilter(String whereClause, String[] tableColumnsArray, List<Map<String, byte[]>> rows) {
        if (whereClause == null || whereClause.isEmpty()) {
            return;
        }
        WhereFilter filter = new WhereFilter();
        filter.filter(whereClause, tableColumnsArray, rows);
    }

    private List<Map<String, byte[]>> getRows(RandomAccessFile dbFile, int pageSize, int rootPage, String[] tableColumnsArray, SQLQueryParser parser) throws IOException {
        List<Map<String, byte[]>> rows = new ArrayList<>();
        long originalPosition = dbFile.getFilePointer();

        // Read the B-tree page type
        byte bTreePageType = getBTreePageType(dbFile, pageSize, rootPage);
        int numberOfCells = getNumberOfCells(dbFile);

        // Check if this is an interior table or a leaf table
        if (isInteriorTable(bTreePageType)) {
            // Process interior page
            rows.addAll(handleInteriorPage(dbFile, pageSize, numberOfCells, tableColumnsArray, rootPage, parser));
        } else if (isLeafTable(bTreePageType)) {
            // Process leaf page
            rows.addAll(handleLeafPage(dbFile, pageSize, numberOfCells, tableColumnsArray, rootPage, parser));
        }

        dbFile.seek(originalPosition);
        return rows;
    }

    // Method to handle leaf pages (existing logic for extracting rows)
    private List<Map<String, byte[]>> handleLeafPage(RandomAccessFile dbFile, int pageSize, int numberOfCells, String[] tableColumnsArray, int rootPage, SQLQueryParser parser) throws IOException {
        List<Map<String, byte[]>> rows = new ArrayList<>();
        long startOfPage = (long) (rootPage - 1) * pageSize;

        int[] cellPointers = getCellPointers(numberOfCells, dbFile);
        for (int i = 0; i < numberOfCells; i++) {
            long seekValue = startOfPage + cellPointers[i];
            dbFile.seek(seekValue);
            Map<String, byte[]> rowData = new HashMap<>();
            List<VarInt> serialTypes = getSerialTypes(dbFile);
            List<byte[]> values = getSerializedValues(dbFile, serialTypes, dbFile.getFilePointer());
            for (int j = 0; j < tableColumnsArray.length; j++) {
                rowData.put(tableColumnsArray[j], values.get(j));
            }
            //convert the Row ID into array of bytes
            String rowID = String.valueOf(ROWID.value);
            byte[] bytes = rowID.getBytes();
            rowData.put(parser.primaryKey, bytes);
            rows.add(rowData);
        }

        return rows;
    }

    // Method to handle interior pages (new logic to recursively handle child pages)
    private List<Map<String, byte[]>> handleInteriorPage(RandomAccessFile dbFile, int pageSize, int numberOfCells, String[] tableColumnsArray, int pageNumber, SQLQueryParser parser) throws IOException {
        List<Map<String, byte[]>> rows = new ArrayList<>();
        long startOfPage = (long) (pageNumber - 1) * pageSize;

        // Read the right-most pointer
        int rightMostPointer = dbFile.readInt();

        // Read cell pointers
        int[] cellPointers = getCellPointers(numberOfCells, dbFile);

        // Recursively process child pages
        for (int i = 0; i < numberOfCells; i++) {
            dbFile.seek(startOfPage + cellPointers[i]);
            int childPageNumber = dbFile.readInt();
            rows.addAll(getRows(dbFile, pageSize, childPageNumber, tableColumnsArray, parser));  // Recursively retrieve rows from child pages
            VarInt.readVarInt(dbFile);  // Skipping the VarInt after processing child page
        }

        // Process the right-most child page
        rows.addAll(getRows(dbFile, pageSize, rightMostPointer, tableColumnsArray, parser));

        return rows;
    }


    private int getTableColumnIndex(String column, String[] columnsArray) {
        for (int i = 0; i < columnsArray.length; i++) {
            if (columnsArray[i].trim().equalsIgnoreCase(column)) {
                return i;
            }
        }
        return -1;
    }

    private Map<String, byte[]> findTableRootPage(RandomAccessFile dbFile, int pageSize, String tableName, int pageNumber) throws IOException {
        long originalPosition = dbFile.getFilePointer();
        try {
            byte bTreePageType = getBTreePageType(dbFile, pageSize, pageNumber);
            int numberOfCells = getNumberOfCells(dbFile);
            Map<String, byte[]> tableInfo = new HashMap<>();
            if (isInteriorTable(bTreePageType)) {
                tableInfo = handleFindRootPageInterior(dbFile, pageSize, numberOfCells, tableName, pageNumber);
            } else if (isLeafTable(bTreePageType)) {
                tableInfo = handleFindRootPageLeaf(dbFile, numberOfCells, tableName, pageNumber, pageSize);
            }
            dbFile.seek(originalPosition);
            return tableInfo;
        } catch (IOException e) {
            printError("Error reading file: " + e.getMessage());
        } finally {
            try {
                dbFile.seek(originalPosition);
            } catch (IOException e) {
                printError("Error seeking file: " + e.getMessage());
            }
        }
        return new HashMap<>();
    }

    private Map<String, byte[]> handleFindRootPageLeaf(RandomAccessFile dbFile, int numberOfCells, String tableName, int pageNumber, int pageSize) throws IOException {
        int[] cellPointers = getCellPointers(numberOfCells, dbFile);
        long startOfPage = (long) (pageNumber - 1) * pageSize;
        for (int i = 0; i < numberOfCells; i++) {
            long seekValue = startOfPage + cellPointers[i];
            dbFile.seek(seekValue);
            Map<String, byte[]> cellData = getCellData(dbFile);
            String currentTableName = new String(cellData.get("tbl_name"), StandardCharsets.UTF_8);
            if (currentTableName.equals(tableName)) {
                return cellData;
            }
        }
        return new HashMap<>();
    }

    private Map<String, byte[]> handleFindRootPageInterior(RandomAccessFile dbFile, int pageSize, int numberOfCells, String tableName, int pageNumber) throws IOException {
        long startOfPage = (long) (pageNumber - 1) * pageSize;
        int rightMostPointer = dbFile.readInt();
        int[] cellPointers = getCellPointers(numberOfCells, dbFile);
        for (int i = 0; i < numberOfCells; i++) {
            Map<String, byte[]> tableInfo = processCell(dbFile, startOfPage, cellPointers[i], pageSize, tableName);
            if (!tableInfo.isEmpty()) {
                return tableInfo;
            }
        }

        return handleRightMostPointer(dbFile, pageSize, tableName, rightMostPointer);
    }

    private Map<String, byte[]> processCell(RandomAccessFile dbFile, long startOfPage, int cellPointer, int pageSize, String tableName) throws IOException {
        dbFile.seek(startOfPage + cellPointer);
        int childPageNumber = dbFile.readInt();
        Map<String, byte[]> tableInfo = findTableRootPage(dbFile, pageSize, tableName, childPageNumber);
        if (tableInfo.isEmpty()) {
            VarInt.readVarInt(dbFile); // Skipping the VarInt after processing child page
        }
        return tableInfo;
    }

    private Map<String, byte[]> handleRightMostPointer(RandomAccessFile dbFile, int pageSize, String tableName, int rightMostPointer) throws IOException {
        return findTableRootPage(dbFile, pageSize, tableName, rightMostPointer);
    }
}