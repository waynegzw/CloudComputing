import java.sql.*;

public class MySQLTasks {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "song_db";
    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME;

    private static final String DB_USER = "root";
    private static final String DB_PWD = "db15319root";

    private static Connection conn;

    /**
     * You should complete the missing parts in the following method. Feel free to add helper functions if necessary.
     * <p/>
     * For all questions, output your answer in one single line, i.e. use System.out.print().
     *
     * @param args The arguments for main method.
     */
    public static void main(String[] args) {
        try {
            initializeConnection();
            // This argument should be used to determine the piece(s) of your code to run.
            String runOption = args[0];
            switch (runOption) {
                // Run the demo function.
                case "demo":
                    demo();
                    break;
                // Load data from the csv files into corresponding tables.
                case "load_data":
                    loadData();
                    break;
                // Answer question 7.
                case "q7":
                    q7();
                    break;
                // Answer question 8.
                case "q8":
                    // For q8, there should be an args[1] which is the name (NOT field) of your intended database index.
                    q8(args[1]);
                    break;
                // Answer question 9.
                case "q9":
                    q9();
                    break;
                // Answer question 10.
                case "q10":
                    q10();
                    break;
                // Answer question 11.
                case "q11":
                    q11();
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Initializes database connection.
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private static void initializeConnection() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
    }

    /**
     * JDBC usage demo. The following function will print the row count of the "songs" table.
     * Table must exists before this function is called.
     */
    private static void demo() {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String tableName = "songs";
            String sql = "SELECT count(*) AS cnt FROM " + tableName;
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                int rowCount = rs.getInt("cnt");
                System.out.println("Total number of lines in " + tableName + " is: " + rowCount);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Load data.
     * <p/>
     * This method should load data from csv files into corresponding tables.
     * Complete this method with your own implementation.
     * <p/>
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void loadData() {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String sql = "LOAD DATA LOCAL INFILE 'million_songs_metadata.csv' INTO TABLE songs
FIELDS TERMINATED BY ',';";
            boolean rs = stmt.execute(sql);

            sql = "LOAD DATA LOCAL INFILE 'million_songs_sales_data.csv' INTO TABLE sales
FIELDS TERMINATED BY ',';";
            rs = stmt.execute(sql);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Question 7.
     * <p/>
     * This method should execute a SQL query and print the trackid of the song with the maximum duration.
     * If there are multiple answers, simply print any one of them. Do NOT hardcode your answer.
     * <p/>
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q7() {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String tableName = "songs";
            String sql = "SELECT track_id FROM songs " +
                    "WHERE duration = (SELECT MAX(duration) FROM songs);";
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                String trackId = rs.getString("track_id");
                System.out.println(trackId);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Question 8.
     * <p/>
     * A database index is a data structure that improves the speed of data retrieval.
     * Identify the field that will improve the performance of your query in question 7
     * and create a database index on that field. A custom index name is needed to create an index.
     * <p/>
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     *
     * @param indexName The name of your index (this is NOT the field on which your index will be created).
     */
    private static void q8(String indexName) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String sql = "CREATE INDEX duration_index ON songs(duration);";
            boolean rs = stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Question 9.
     * <p/>
     * This method should execute a SQL query and return the trackid of the song with the maximum duration.
     * If there are multiple answers, simply print any one of them. Do NOT hardcode your answer.
     * <p/>
     * This is the same query as Question 7. Do you see any difference in performance?
     * <p/>
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q9() {
        q7();
    }

    /**
     * Question 10.
     * <p/>
     * Write the SQL query that returns all matches (across any column), similar to the command grep -P 'The Beatles' | wc -l:
     * Do NOT hardcode your answer.
     * <p/>
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q10() {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String sql = "SELECT count(track_id) FROM songs " +
                    "WHERE BINARY artist_name LIKE '%The Beatles%' " +
                    "OR BINARY title LIKE '%The Beatles%' " +
                    "OR BINARY songs.release LIKE '%The Beatles%';";
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                int cnt = rs.getInt(1);
                System.out.println(cnt);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Question 11.
     * <p/>
     * Which artist has the third-most number of rows in table songs? The output should be the name of the artist.
     * Please use artist_id as the unique identifier of the artist.
     * If there are multiple answers, simply print any one of them. Do NOT hardcode your answer.
     * <p/>
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q11() {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String sql = "SELECT artist_name FROM songs " +
                    "GROUP BY artist_id " +
                    "ORDER BY COUNT(track_id) DESC " +
                    "LIMIT 2, 1;";
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                String name = rs.getString(1);
                System.out.println(name);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
