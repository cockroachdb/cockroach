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

        String DB_URL = "jdbc:postgresql://";
        DB_URL += System.getenv("PGHOST") + ":" + System.getenv("PGPORT");
        DB_URL += "/test?ssl=true";
        DB_URL += "&sslcert=" + System.getenv("PGSSLCERT");
        DB_URL += "&sslkey=key.pk8";
        DB_URL += "&sslrootcert=/certs/ca.crt";
        DB_URL += "&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory";
        Connection conn;
        conn = DriverManager.getConnection(DB_URL);
        PreparedStatement stmt = conn.prepareStatement("CREATE DATABASE test");
        int res = stmt.executeUpdate();
        if (res != 0) {
            throw new Exception("unexpected: CREATE DATABASE reports " + res + " rows changed, expecting 0");
        }

        stmt = conn.prepareStatement("USE test");
        stmt.executeUpdate();

        this.conn = conn;
    }

    @After
    public void cleanup() throws Exception {
        PreparedStatement stmt = this.conn.prepareStatement("DROP DATABASE test");
        stmt.execute();
    }
}
