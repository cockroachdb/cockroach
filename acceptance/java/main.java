import java.sql.*;

public class main {
    public static void main(String[] args) throws Exception {
        Connection conn = TestConnection.getConnection();
        TestStatements.runTests(conn)
    }
}
