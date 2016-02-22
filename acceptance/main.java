import java.sql.*;

public class main {
    public static void main(String[] args) throws Exception {

	Connection conn = Test.getConnection();
	Class.forName("org.postgresql.Driver");
	    
	String DB_URL = "jdbc:postgresql://";
	DB_URL += System.getenv("PGHOST") + ":" + System.getenv("PGPORT");
	DB_URL += "/?ssl=true";
	DB_URL += "&sslcert=" + System.getenv("PGSSLCERT");
	DB_URL += "&sslkey=key.pk8";
	DB_URL += "&sslrootcert=/certs/ca.crt";
	DB_URL += "&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory";
	Connection conn = DriverManager.getConnection(DB_URL);

	PreparedStatement stmt = conn.prepareStatement("SELECT 1, 2 > ?, ?::int, ?::string, ?::string, ?::string, ?::string, ?::string");
	stmt.setInt(1, 3);
	stmt.set%v;

	stmt.setBoolean(3, true);
	stmt.setLong(4, -4L);
	stmt.setFloat(5, 5.31f);
	stmt.setDouble(6, -6.21d);
	stmt.setShort(7, (short)7);

	ResultSet rs = stmt.executeQuery();
	rs.next();
	int a = rs.getInt(1);
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
    }
}
