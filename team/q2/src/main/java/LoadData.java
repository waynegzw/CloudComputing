import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by shenanqi on 3/11/16.
 */
public class LoadData {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "twitters";
    //private static final String DB_NAME = "byte3_aware";
    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME;
    private static final String COMMA_DELIMITER = "\t";
    private static final String DB_USER = "ubuntu";
    private static final String DB_PWD = "teamproject";
    //private static final String DB_PWD = "shen070807";
    private static Connection conn;

    public static void main(String[] args) {
        try {
            initializeConnection();
            loadData(args[0]);
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

    private static void initializeConnection() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
    }


    private static void loadData(String file) {

        BufferedReader fileReader1 = null;
        try {
            String sql=null;
            String line = "";
            // Create the file reader
            fileReader1 = new BufferedReader(new FileReader(file));
            PreparedStatement preparedStmt =null;

            // Read the file line by line starting from the second line
            while ((line = fileReader1.readLine()) != null) {

                // Get all tokens available in line
                String[] tokens = line.split(COMMA_DELIMITER);
                // this query to insert the value
                sql="Insert IGNORE into twitter_tb (user_id, hashtag, twitter_id,score,ttime,content) values (?,?,?,?,?,?);";
                preparedStmt = conn.prepareStatement(sql);
                try{
                    preparedStmt.setString (1, tokens[0]);
                    preparedStmt.setString (2, tokens[1]);
                    preparedStmt.setString (3, tokens[2]);
                    preparedStmt.setString(4, tokens[3]);
                    preparedStmt.setString(5, tokens[4]);
                    preparedStmt.setString(6, tokens[5]);
                    //  preparedStmt.setString(7, tokens[6]);
                    preparedStmt .executeUpdate();
                }finally {
                    preparedStmt.close();
                }

            }

        } catch (Exception e) {
            System.out.println("Error in CsvFileReader !!!");
            e.printStackTrace();
        } finally {
            try {
                fileReader1.close();
            } catch (IOException e) {
                System.out.println("Error while closing fileReader !!!");
                e.printStackTrace();
            }
        }
    }
}

