package com.example.hive;

import java.sql.*;
import java.util.Properties;

/**
 * Example class to connect to Hive and query the sales table
 */
public class HiveQueryExample {
    
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String CONNECTION_URL = "jdbc:hive2://localhost:10000";
    private static final String USERNAME = "hive";
    private static final String PASSWORD = ""; // Hive doesn't require password for local connections
    
    public static void main(String[] args) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        
        try {
            // Load the Hive JDBC driver
            Class.forName(HIVE_DRIVER);
            System.out.println("Hive JDBC driver loaded successfully");
            
            // Create connection properties
            Properties properties = new Properties();
            properties.setProperty("user", USERNAME);
            properties.setProperty("password", PASSWORD);
            
            // Establish connection to Hive
            System.out.println("Connecting to Hive at: " + CONNECTION_URL);
            connection = DriverManager.getConnection(CONNECTION_URL, properties);
            System.out.println("Connected to Hive successfully!");
            
            // Create statement
            statement = connection.createStatement();
            
            // First, let's create a sample sales table if it doesn't exist
            createSalesTableIfNotExists(statement);
            
            // Insert some sample data if the table is empty
            insertSampleData(statement);
            
            // Query the sales table
            System.out.println("\nQuerying sales table...");
            String query = "SELECT * FROM sales ORDER BY sale_date DESC";
            resultSet = statement.executeQuery(query);
            
            // Print column headers
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            System.out.println("\nSales Table Results:");
            System.out.println("===================");
            
            // Print column names
            for (int i = 1; i <= columnCount; i++) {
                System.out.printf("%-15s", metaData.getColumnName(i));
            }
            System.out.println();
            
            // Print separator line
            for (int i = 1; i <= columnCount; i++) {
                System.out.printf("%-15s", "---------------");
            }
            System.out.println();
            
            // Print data rows
            int rowCount = 0;
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.printf("%-15s", resultSet.getString(i));
                }
                System.out.println();
                rowCount++;
            }
            
            System.out.println("\nTotal rows returned: " + rowCount);
            
        } catch (ClassNotFoundException e) {
            System.err.println("Error: Hive JDBC driver not found!");
            System.err.println("Make sure the hive-jdbc dependency is included in your pom.xml");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Error connecting to Hive or executing query:");
            System.err.println("SQL State: " + e.getSQLState());
            System.err.println("Error Code: " + e.getErrorCode());
            System.err.println("Message: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Unexpected error occurred:");
            e.printStackTrace();
        } finally {
            // Close resources
            try {
                if (resultSet != null) resultSet.close();
                if (statement != null) statement.close();
                if (connection != null) connection.close();
                System.out.println("\nDatabase connection closed.");
            } catch (SQLException e) {
                System.err.println("Error closing database resources:");
                e.printStackTrace();
            }
        }
    }
    
    /**
     * Create the sales table if it doesn't exist
     */
    private static void createSalesTableIfNotExists(Statement statement) throws SQLException {
        String createTableSQL = 
            "CREATE TABLE IF NOT EXISTS sales (" +
            "    id INT, " +
            "    product_name STRING, " +
            "    sale_date DATE, " +
            "    amount DECIMAL(10,2), " +
            "    customer_id INT" +
            ") " +
            "ROW FORMAT DELIMITED " +
            "FIELDS TERMINATED BY ',' " +
            "STORED AS TEXTFILE";
        
        System.out.println("Creating sales table if not exists...");
        statement.execute(createTableSQL);
        System.out.println("Sales table created/verified successfully");
    }
    
    /**
     * Insert sample data into the sales table
     */
    private static void insertSampleData(Statement statement) throws SQLException {
        // Check if table has data
        ResultSet countResult = statement.executeQuery("SELECT COUNT(*) as count FROM sales");
        countResult.next();
        int rowCount = countResult.getInt("count");
        countResult.close();
        
        if (rowCount == 0) {
            System.out.println("Inserting sample data into sales table...");
            
            String[] insertStatements = {
                "INSERT INTO sales VALUES (1, 'Laptop', '2024-01-15', 999.99, 101)",
                "INSERT INTO sales VALUES (2, 'Mouse', '2024-01-16', 29.99, 102)",
                "INSERT INTO sales VALUES (3, 'Keyboard', '2024-01-17', 79.99, 103)",
                "INSERT INTO sales VALUES (4, 'Monitor', '2024-01-18', 299.99, 101)",
                "INSERT INTO sales VALUES (5, 'Headphones', '2024-01-19', 149.99, 104)",
                "INSERT INTO sales VALUES (6, 'Laptop', '2024-01-20', 1099.99, 105)",
                "INSERT INTO sales VALUES (7, 'Mouse', '2024-01-21', 39.99, 102)",
                "INSERT INTO sales VALUES (8, 'Tablet', '2024-01-22', 499.99, 106)"
            };
            
            for (String insertSQL : insertStatements) {
                statement.execute(insertSQL);
            }
            
            System.out.println("Sample data inserted successfully");
        } else {
            System.out.println("Sales table already contains data (" + rowCount + " rows)");
        }
    }
}
