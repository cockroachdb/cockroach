import java.sql.*;
    
public class TestStatements {
    public static void runTests(Connection conn) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("SELECT 1, 2 > ?, ?::int");
        stmt.setInt(1, 3);
        stmt.setString(2, "a"); // invalid because wrong type

        ResultSet rs = stmt.executeQuery();
        rs.next();
    }
}
