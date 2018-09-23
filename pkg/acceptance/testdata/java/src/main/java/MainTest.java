import org.junit.*;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MainTest extends CockroachDBTest {
    @Test
    public void testNoOp() throws Exception {
      // This is a test we can target when building the Docker image to ensure
      // Maven downloads all packages necessary for testing before we enter
      // offline mode. (Maven dependency:go-offline is not bulletproof, and
      // leaves out surefire-junit.)
    }

    @Test
    public void testSimpleTable() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE f (x INT, ts TIMESTAMP)");
        int res = stmt.executeUpdate();
        Assert.assertEquals(0, res);

        stmt = conn.prepareStatement("INSERT INTO test.f VALUES (42, timestamp '2015-05-07 18:20:00')");
        res = stmt.executeUpdate();
        Assert.assertEquals(1, res);

        stmt = conn.prepareStatement("SELECT * FROM f");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Assert.assertEquals(42, rs.getInt(1));
        Assert.assertEquals("2015-05-07 18:20:00.0", rs.getTimestamp(2).toString());
    }

    @Test
    public void testBindParameters() throws Exception {
        PreparedStatement stmt = conn.prepareStatement(
                "SELECT 1, 2 > ?, ?::int, ?::string, ?::string, ?::string, ?::string, ?::string"
        );
        stmt.setInt(1, 3);
        stmt.setInt(2, 3);
        stmt.setBoolean(3, true);
        stmt.setLong(4, -4L);
        stmt.setFloat(5, 5.31f);
        stmt.setDouble(6, -6.21d);
        stmt.setShort(7, (short) 7);

        ResultSet rs = stmt.executeQuery();
        rs.next();
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals(false, rs.getBoolean(2));
        Assert.assertEquals(3, rs.getInt(3));
        Assert.assertEquals("true", rs.getString(4));
        Assert.assertEquals(-4, rs.getLong(5));
        Assert.assertEquals(5.31, rs.getFloat(6), 0.01);
        Assert.assertEquals(-6.21, rs.getFloat(7), 0.01);
        Assert.assertEquals(7, rs.getShort(8));
    }

    @Test
    public void testInsertWithParameters() throws Exception {
        PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT, cdate DATE)"
        );
        int res = stmt.executeUpdate();
        Assert.assertEquals(0, res);

        stmt = conn.prepareStatement("INSERT INTO accounts (id, balance, cdate) VALUES ( ?, ?, ? )");
        stmt.setObject(1, 1);
        stmt.setObject(2, 1000);
        stmt.setObject(3, new java.sql.Date(System.currentTimeMillis()));
        stmt.executeUpdate();

        stmt = conn.prepareStatement("SELECT * FROM accounts");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals(1000, rs.getInt(2));
        Assert.assertEquals(3, rs.getMetaData().getColumnCount());
    }

    @Test
    public void testUpdate() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE str (s STRING)");
        stmt.executeUpdate();
        stmt = conn.prepareStatement("INSERT INTO str VALUES ('hello')");
        stmt.executeUpdate();
        stmt = conn.prepareStatement("UPDATE str SET s = ?");
        stmt.setString(1, "world");
        stmt.execute();
        stmt = conn.prepareStatement("SELECT * FROM str");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Assert.assertEquals("world", rs.getString(1));
    }

    @Test
    public void testEmptyTable() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE empty()");
        int res = stmt.executeUpdate();
        Assert.assertEquals(0, res);

        stmt = conn.prepareStatement("SELECT * FROM empty");
        ResultSet rs = stmt.executeQuery();
        Assert.assertEquals(0, rs.getMetaData().getColumnCount());
    }

    @Test
    public void testDecimal() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("SELECT 1.0::DECIMAL");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        // The JDBC Postgres driver's getObject has different behavior than both
        // its getString and getBigDecimal with respect to how it handles the type modifier:
        // https://github.com/pgjdbc/pgjdbc/blob/REL42.1.1/pgjdbc/src/main/java/org/postgresql/jdbc/PgResultSet.java#L188
        Object dec = rs.getObject(1);
        Assert.assertEquals("1.0", dec.toString());

        stmt = conn.prepareStatement("SELECT 1e1::DECIMAL");
        rs = stmt.executeQuery();
        rs.next();
        BigDecimal bigdec = rs.getBigDecimal(1);
        Assert.assertEquals("1E+1", bigdec.toString());
    }

    @Test
    public void testTime() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("SELECT '01:02:03.456'::TIME");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        String actual = new SimpleDateFormat("HH:mm:ss.SSS").format(rs.getTime(1));
        Assert.assertEquals("01:02:03.456", actual);
    }

    @Test
    public void testUUID() throws Exception {
        UUID uuid = UUID.randomUUID();
        PreparedStatement stmt = conn.prepareStatement("SELECT ?");
        stmt.setObject(1, uuid);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Assert.assertEquals(uuid, rs.getObject(1));
    }

    @Test
    public void testArrays() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("SELECT ?");
        Array array = conn.createArrayOf("FLOAT", new Double[]{1.0, 2.0, 3.0});
        stmt.setArray(1, array);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Array ar = rs.getArray(1);
        Double[] fs = (Double[]) ar.getArray();
        Assert.assertArrayEquals(new Double[]{1.0, 2.0, 3.0}, fs);
    }

    @Test
    public void testArrayWithProps() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE x (a SMALLINT[])");
        stmt.execute();
        stmt = conn.prepareStatement("INSERT INTO x VALUES (ARRAY[123])");
        stmt.execute();
        stmt = conn.prepareStatement("SELECT a FROM x");
        ResultSet rs = stmt.executeQuery();
        rs.next();

        Array ar = rs.getArray(1);
        Long[] fs = (Long[]) ar.getArray();
        Assert.assertArrayEquals(new Long[]{123L}, fs);
    }

    @Test
    public void testStringArray() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("SELECT '{123,\"hello\",\"\\\"hello\\\"\"}'::STRING[]");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        String[] result = (String[])rs.getArray(1).getArray();
        Assert.assertArrayEquals(new String[]{"123", "hello", "\"hello\""}, result);
    }

    @Test
    public void testSequence() throws Exception {
        PreparedStatement createSeq = conn.prepareStatement("CREATE SEQUENCE foo");
        int res = createSeq.executeUpdate();
        Assert.assertEquals(res, 0);

        PreparedStatement getNextVal = conn.prepareStatement("SELECT nextval('foo')");

        ResultSet rs = getNextVal.executeQuery();
        rs.next();
        Assert.assertEquals(rs.getInt(1), 1);

        rs = getNextVal.executeQuery();
        rs.next();
        Assert.assertEquals(rs.getInt(1), 2);
    }
}
