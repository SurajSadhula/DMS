import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataMigration {

    private final JdbcTemplate mysqlTemplate;
    private final JdbcTemplate postgresTemplate;
    private final List<Map<String, Object>> failedRows = new ArrayList<>();

    public DataMigration(DataSource mysqlDataSource, DataSource postgresDataSource) {
        this.mysqlTemplate = new JdbcTemplate(mysqlDataSource);
        this.postgresTemplate = new JdbcTemplate(postgresDataSource);
    }

    public void migrateData(String mysqlTable, String postgresTable) {
        int offset = 0;
        int limit = 1000;
        boolean hasMoreData = true;

        long startTime = System.currentTimeMillis(); // Start the timer

        try {
            disableConstraints(postgresTable); // Disable constraints

            while (hasMoreData) {
                try {
                    String queryMySQL = "SELECT * FROM " + mysqlTable + " LIMIT " + limit + " OFFSET " + offset;
                    List<Map<String, Object>> rows = mysqlTemplate.query(queryMySQL, (RowMapper<Map<String, Object>>) (rs, rowNum) -> {
                        int columnCount = rs.getMetaData().getColumnCount();
                        Map<String, Object> row = new HashMap<>();
                        for (int i = 1; i <= columnCount; i++) {
                            row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                        }
                        return row;
                    });

                    if (rows.isEmpty()) {
                        hasMoreData = false;
                        break;
                    }

                    for (Map<String, Object> row : rows) {
                        try {
                            String insertPostgreSQL = "INSERT INTO " + postgresTable + " VALUES (" +
                                    String.join(", ", row.values().stream().map(value -> "'" + value + "'").toArray(String[]::new)) + ")";
                            postgresTemplate.update(insertPostgreSQL);
                        } catch (Exception e) {
                            failedRows.add(row);
                            System.err.println("Insertion failed for row: " + row + " - " + e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Failed to fetch data from MySQL with OFFSET " + offset + " - " + e.getMessage());
                }

                offset += limit;
            }

            System.out.println("Data migration completed successfully!");
        } finally {
            enableConstraints(postgresTable); // Re-enable constraints
        }

        long endTime = System.currentTimeMillis(); // Stop the timer
        System.out.println("Total time taken: " + (endTime - startTime) + " ms");

        if (!failedRows.isEmpty()) {
            System.out.println("Failed rows during migration:");
            failedRows.forEach(row -> System.out.println(row));
        } else {
            System.out.println("No failed rows detected.");
        }
    }

    private void disableConstraints(String tableName) {
        postgresTemplate.update("ALTER TABLE " + tableName + " DISABLE TRIGGER ALL");
        System.out.println("Constraints disabled for table: " + tableName);
    }

    private void enableConstraints(String tableName) {
        postgresTemplate.update("ALTER TABLE " + tableName + " ENABLE TRIGGER ALL");
        System.out.println("Constraints re-enabled for table: " + tableName);
    }
}
