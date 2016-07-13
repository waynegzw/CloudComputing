import java.io.*;
import java.sql.*;

/**
 * Created by xgy on 12/03/16.
 */
public class Checker {
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "twitters";
    //private static final String DB_NAME = "byte3_aware";
    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME + "";
    private static final String COMMA_DELIMITER = "\t";
    private static final String DB_USER = "ubuntu";
    private static final String DB_PWD = "teamproject";
    //private static final String DB_PWD = "shen070807";
    private static Connection conn;

    public static void main(String[] args) {
        try {
            initializeConnection();
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("testdup"), "UTF8"));
            PrintStream out2 = new PrintStream(System.out, true, "UTF-8");

            String sql = "SELECT * FROM twitter_tb WHERE twitter_id = ? AND hashtag = ?";
            String line = "";
            // Create the file reader
            PreparedStatement preparedStmt = null;

            String input;
            while ((input = br.readLine()) != null) {
                preparedStmt = conn.prepareStatement(sql);
                String[] token = input.split("\t");
                String tag = token[1];
                String tid = token[2];
                preparedStmt.setString(1, tid);
                preparedStmt.setString(2, tag);

                ResultSet rs = preparedStmt.executeQuery();
                if (!rs.next()) {
                    out2.println("NOT FOUND: tag: " + tag + "tid: " + tid);
                }
            }
        } catch (SQLException s) {
            s.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    private static void initializeConnection() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
    }
}
