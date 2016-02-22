import java.sql.*;

public class TestConnection {
    public static Connection getConnection() {
        Class.forName("org.postgresql.Driver");

        String DB_URL = "jdbc:postgresql://";
        DB_URL += System.getenv("PGHOST") + ":" + System.getenv("PGPORT");
        DB_URL += "/?ssl=true";
        DB_URL += "&sslcert=" + System.getenv("PGSSLCERT");
        DB_URL += "&sslkey=key.pk8";
        DB_URL += "&sslrootcert=/certs/ca.crt";
        DB_URL += "&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory";
        return DriverManager.getConnection(DB_URL);
    }
}
