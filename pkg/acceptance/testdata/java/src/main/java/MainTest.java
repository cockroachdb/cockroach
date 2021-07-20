import org.junit.*;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static junit.framework.TestCase.fail;
import org.postgresql.util.PSQLException;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MainTest extends CockroachDBTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

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
    public void testIntTypes() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE x (a SMALLINT, b INT4, c INT, d BIGINT)");
        stmt.execute();
        stmt = conn.prepareStatement("INSERT INTO x VALUES (1, 1, 1, 1)");
        stmt.execute();
        stmt = conn.prepareStatement("SELECT a, b, c, d FROM x");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Short s = rs.getShort(1);
        Assert.assertEquals(1, (short)s);
        Integer i = rs.getInt(2);
        Assert.assertEquals(1, (int)i);
        Long l = rs.getLong(3);
        Assert.assertEquals(1L, (long)l);
        l = rs.getLong(4);
        Assert.assertEquals(1L, (long)l);
    }

    @Test
    public void testFloatTypes() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE x (a FLOAT4, b FLOAT8)");
        stmt.execute();
        stmt = conn.prepareStatement("INSERT INTO x VALUES (1.2, 1.2)");
        stmt.execute();
        stmt = conn.prepareStatement("SELECT a, b FROM x");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Float f = rs.getFloat(1);
        Assert.assertEquals((float)1.2, (float)f, 0.0001);
        Double d = rs.getDouble(2);
        Assert.assertEquals(1.2, (double)d, 0.0001);
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
    public void testTimeTZ() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("SELECT '01:02:03.456-07:00'::TIMETZ");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        String actual = new SimpleDateFormat("HH:mm:ss.SSSZ").format(rs.getTime(1));
        Assert.assertEquals("08:02:03.456+0000", actual);
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
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE x (a SMALLINT[], b INT4[], c BIGINT[])");
        stmt.execute();
        stmt = conn.prepareStatement("INSERT INTO x VALUES (ARRAY[123], ARRAY[123], ARRAY[123])");
        stmt.execute();
        stmt = conn.prepareStatement("SELECT a, b, c FROM x");
        ResultSet rs = stmt.executeQuery();
        rs.next();

        Array ar = rs.getArray(1);
        Integer[] fs = (Integer[]) ar.getArray();
        Assert.assertArrayEquals(new Integer[]{123}, fs);
        ar = rs.getArray(2);
        fs = (Integer[]) ar.getArray();
        Assert.assertArrayEquals(new Integer[]{123}, fs);
        ar = rs.getArray(3);
        Long[] longs = (Long[]) ar.getArray();
        Assert.assertArrayEquals(new Long[]{123L}, longs);
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

    @Test
    public void selectLimitExplicitTxn() throws Exception {
        conn.setAutoCommit(false);
        PreparedStatement stmt = conn.prepareStatement("SELECT * from generate_series(1, 10)");
        stmt.setFetchSize(1);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Assert.assertEquals(1, rs.getInt(1));
        rs.next();
        Assert.assertEquals(2, rs.getInt(1));
        rs.setFetchSize(0);
        rs.next();
        Assert.assertEquals(3, rs.getInt(1));
        conn.setAutoCommit(true);
    }

    // Regression for 30538: SQL query with wrong parameter value crashes
    // database. Unlike the Go client, the JDBC client sets placeholder type
    // hints. When these do not match the types inferred during type checking,
    // placeholder eval will try to convert to the needed type. If the conversion
    // fails, AssignPlaceholders needs to gracefully report that error rather than
    // panicking.
    @Test
    public void testPlaceholderTypeError() throws Exception {
        PreparedStatement stmt1 = conn.prepareStatement("CREATE TABLE x (a INT PRIMARY KEY, b UUID)");
        stmt1.execute();

        // Send a UUID that's malformed (not long enough) and expect error.
        PreparedStatement stmt = conn.prepareStatement("SELECT * FROM x WHERE b = ?");
        stmt.setObject(1, "e81bb788-2291-4b6e-9cf3-b237fe6c2f3");
        exception.expectMessage("ERROR: could not parse \"e81bb788-2291-4b6e-9cf3-b237fe6c2f3\" as " +
            "type uuid: uuid: incorrect UUID format: e81bb788-2291-4b6e-9cf3-b237fe6c2f3");
        stmt.executeQuery();
    }

    // Regression for 33340: temporary columns used by window functions should
    // be projected out after they are no longer necessary. This problem
    // presented itself only when invoked via an external driver by crashing
    // the server.
    @Test
    public void testWindowFunctions() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE t (a INT, b INT)");
        stmt.execute();
        stmt = conn.prepareStatement("INSERT INTO t VALUES (0, 10), (1, 11)");
        stmt.execute();
        stmt = conn.prepareStatement("SELECT sum(b) OVER (ORDER BY a) FROM t");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Assert.assertEquals(rs.getInt(1), 10);
        rs.next();
        Assert.assertEquals(rs.getInt(1), 21);
    }

    // Regression for 34429: empty string decimals shouldn't crash the server.
    @Test
    public void testEmptyStringDec() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("create table product_temp_tb (product_master_id int primary key,  weight DECIMAL(10,2) NOT NULL DEFAULT 0)");
        stmt.execute();
        stmt = conn.prepareStatement("INSERT INTO product_temp_tb values(0,0)");
        stmt.execute();
        stmt = conn.prepareStatement("UPDATE product_temp_tb SET weight = ? WHERE product_master_id = ?");
        stmt.setObject(1, "");
        stmt.setInt(2, 1);
        exception.expectMessage("ERROR: could not parse \"\" as type decimal");
        stmt.execute();
    }

    // Regression test for #60533: virtual table OIDs should work even they
    // use a 32-bit int greater than MaxInt32.
    @Test
    public void testVirtualTableMetadata() throws Exception {
      PreparedStatement p = conn.prepareStatement("select oid, proname from pg_proc limit 100");
      p.execute();
      ResultSet r = p.getResultSet();
      while (r.next()) {
        ResultSetMetaData m = r.getMetaData();
        int colCount = m.getColumnCount();
        for (int i = 1; i <= colCount; i++) {
          String tableName = m.getTableName(i);
          Assert.assertEquals("pg_proc", tableName);
        }
      }
    }

    // Regression test for #42912: using setFetchSize in a transaction should
    // not cause issues.
    @Test
    public void testSetFetchSize() throws Exception {
      for (int fetchSize = 0; fetchSize <= 3; fetchSize++) {
        testSetFetchSize(fetchSize, true);
        testSetFetchSize(fetchSize, false);
      }
    }

    private void testSetFetchSize(int fetchSize, boolean useTransaction) throws Exception {
      int expectedResults = fetchSize;
      if (fetchSize == 0 || fetchSize == 3) {
        expectedResults = 2;
      }

      try (final Connection testConn = DriverManager.getConnection(getDBUrl(), "root", "")) {
        testConn.setAutoCommit(!useTransaction);
        try (final Statement stmt = testConn.createStatement()) {
          stmt.setFetchSize(fetchSize);
          ResultSet result = stmt.executeQuery("select n from generate_series(0,1) n");
          for (int i = 0; i < expectedResults; i++) {
            Assert.assertTrue(result.next());
            Assert.assertEquals(i, result.getInt(1));
          }
          if (useTransaction) {
            // This should implicitly close the ResultSet (i.e. portal).
            testConn.commit();
            if (fetchSize != 0 && fetchSize != 3) {
              try {
                result.next();
                fail("expected portal to be closed");
              } catch (PSQLException e) {
                Assert.assertTrue(e.getMessage().contains("unknown portal"));
              }
            }
          }
        }
      }
    }
}
