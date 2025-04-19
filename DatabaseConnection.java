import org.springframework.jdbc.datasource.DriverManagerDataSource;
import javax.sql.DataSource;

public class DatabaseConnection {

    public DataSource getMySQLDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/source_db");
        dataSource.setUsername("mysql_user");
        dataSource.setPassword("mysql_password");
        return dataSource;
    }

    public DataSource getPostgreSQLDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://localhost:5432/destination_db");
        dataSource.setUsername("postgres_user");
        dataSource.setPassword("postgres_password");
        return dataSource;
    }
}
