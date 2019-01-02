import org.junit.After;
import org.junit.Before;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public abstract class CockroachDBTest {
    protected Connection conn;

    @Before
    public void setupConnection() throws Exception {
        Class.forName("org.postgresql.Driver");

        Connection conn;
        conn = DriverManager.getConnection(getDBUrl(), "root", "");
        PreparedStatement stmt = conn.prepareStatement("CREATE DATABASE test");
        int res = stmt.executeUpdate();
        if (res != 0) {
            throw new Exception("unexpected: CREATE DATABASE reports " + res + " rows changed, expecting 0");
        }

        stmt = conn.prepareStatement("USE test");
        stmt.executeUpdate();

        this.conn = conn;
    }

    public String getDBUrl() {
        String DBUrl = "jdbc:postgresql://";
        DBUrl += pgHost() + ":" + pgPort();
        DBUrl += "/test";
        if (System.getenv("PGSSLCERT") != null) {
            DBUrl += "?ssl=true";
            DBUrl += "&sslcert=" + System.getenv("PGSSLCERT");
            DBUrl += "&sslkey=/certs/client.root.key.pk8";
            DBUrl += "&sslrootcert=/certs/ca.crt";
            DBUrl += "&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory";
        } else {
            DBUrl += "?sslmode=disable";
        }
        return DBUrl;
    }

    public String pgHost() {
        String host = System.getenv("PGHOST");
        if (host == null) {
            host = "localhost";
        }
        return host;
    }

    public String pgPort() {
        String port = System.getenv("PGPORT");
        if (port == null) {
            port = "26257";
        }
        return port;
    }

    @After
    public void cleanup() throws Exception {
        PreparedStatement stmt = this.conn.prepareStatement("DROP DATABASE test");
        stmt.execute();
    }
}
