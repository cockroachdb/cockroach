import java.sql.*;

public class JavaSuccessTest {

    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");

        String DB_URL = "jdbc:postgresql://";
        DB_URL += System.getenv("PGHOST") + ":" + System.getenv("PGPORT");
        DB_URL += "/test?ssl=true";
        DB_URL += "&sslcert=" + System.getenv("PGSSLCERT");
        DB_URL += "&sslkey=key.pk8";
        DB_URL += "&sslrootcert=/certs/ca.crt";
        DB_URL += "&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory";
        Connection conn = DriverManager.getConnection(DB_URL);

        PreparedStatement stmt = conn.prepareStatement("CREATE DATABASE test");
        int res = stmt.executeUpdate();
        if (res != 0) {
            throw new Exception("unexpected: CREATE DATABASE reports " + res + " rows changed, expecting 0");
        }
        stmt = conn.prepareStatement("CREATE TABLE test.f (x INT, ts TIMESTAMP)");
        res = stmt.executeUpdate();
        if (res != 0) {
            throw new Exception("unexpected: CREATE TABLE reports " + res + " rows changed, expecting 0");
        }
        stmt = conn.prepareStatement("INSERT INTO test.f VALUES (42, timestamp '2015-05-07 18:20:00')");
        res = stmt.executeUpdate();
        if (res != 1) {
            throw new Exception("unexpected: INSERT reports " + res + " rows changed, expecting 1");
        }
        stmt = conn.prepareStatement("SELECT * FROM test.f");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        int a = rs.getInt(1);
        if (a != 42) {
            throw new Exception("unexpected: SELECT can't find inserted value: read " + a + ", expecting 42");
        }
        String tsStr = rs.getTimestamp(2).toString();
        if (!tsStr.equals("2015-05-07 18:20:00.0")) {
            throw new Exception("unexpected value for ts: "+tsStr);
        }
        stmt = conn.prepareStatement("DROP TABLE test.f");
        res = stmt.executeUpdate();
        if (res != 0) {
            throw new Exception("unexpected: DROP TABLE reports " + res + " rows changed, expecting 0");
        }
        stmt = conn.prepareStatement("SELECT 1, 2 > ?, ?::int, ?::string, ?::string, ?::string, ?::string, ?::string");
        stmt.setInt(1, 3);
        stmt.setInt(2, 3);
        stmt.setBoolean(3, true);
        stmt.setLong(4, -4L);
        stmt.setFloat(5, 5.31f);
        stmt.setDouble(6, -6.21d);
        stmt.setShort(7, (short)7);
        rs = stmt.executeQuery();
        rs.next();
        a = rs.getInt(1);
        boolean b = rs.getBoolean(2);
        int c = rs.getInt(3);
        String d = rs.getString(4);
        String e = rs.getString(5);
        String f = rs.getString(6);
        String g = rs.getString(7);
        String h = rs.getString(8);
        if (a != 1 || b != false || c != 3 || !d.equals("true") || !e.equals("-4") || !f.startsWith("5.3") || !g.startsWith("-6.2") || !h.equals("7")) {
            throw new Exception("unexpected");
        }
        stmt = conn.prepareStatement("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT, cdate DATE)");
        res = stmt.executeUpdate();
        if (res != 0) {
            throw new Exception("unexpected: CREATE TABLE reports " + res + " rows changed, expecting 0");
        }
        stmt = conn.prepareStatement("INSERT INTO accounts (id, balance, cdate) VALUES ( ?, ?, ? )");
        stmt.setObject(1, 1);
        stmt.setObject(2, 1000);
        stmt.setObject(3, new java.sql.Date(System.currentTimeMillis()));
        stmt.executeUpdate();
    }
}
