import java.util.HashMap;
import java.util.Map;

public class ColumnMapper {

    private static final Map<String, String> columnMapping = new HashMap<>();

    static {
        columnMapping.put("TINYINT", "SMALLINT");
        columnMapping.put("TEXT", "TEXT");
        columnMapping.put("DATETIME", "TIMESTAMP");
        columnMapping.put("VARCHAR", "VARCHAR");
        columnMapping.put("BLOB", "BYTEA");
        // Add more mappings as required.
    }

    public String mapColumnType(String mysqlType) {
        return columnMapping.getOrDefault(mysqlType, "TEXT"); // Default to TEXT for unsupported types
    }
}
