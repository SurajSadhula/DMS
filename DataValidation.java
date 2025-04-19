import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

public class DataValidation {

    private final JdbcTemplate postgresTemplate;

    public DataValidation(DataSource postgresDataSource) {
        this.postgresTemplate = new JdbcTemplate(postgresDataSource);
    }

    public void validateData(String postgresTable) {
        int rowCount = postgresTemplate.queryForObject("SELECT COUNT(*) FROM " + postgresTable, Integer.class);
        System.out.println("Row count in table " + postgresTable + ": " + rowCount);

        validateUniqueConstraints(postgresTable);
        validateReferentialIntegrity(postgresTable);

        System.out.println("Validation completed for table: " + postgresTable);
    }

    private void validateUniqueConstraints(String tableName) {
        String query = "SELECT col1, COUNT(*) FROM " + tableName + " GROUP BY col1 HAVING COUNT(*) > 1";
        List<String> duplicates = postgresTemplate.queryForList(query, String.class);
        if (!duplicates.isEmpty()) {
            System.out.println("Duplicate rows found: " + duplicates);
        } else {
            System.out.println("No duplicate rows found.");
        }
    }

    private void validateReferentialIntegrity(String tableName) {
        System.out.println("Foreign key validation is incomplete but must check if references exist!");
    }
}
