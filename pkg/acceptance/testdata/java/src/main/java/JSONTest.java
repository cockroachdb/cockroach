import org.junit.Assert;
import org.junit.Test;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

// jdbc doesn't actually have any special treatment of JSON, so
// let's just make sure the OID doesn't make it blow up or anything.
public class JSONTest extends CockroachDBTest {
    @Test
    public void testSelectJSON() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("SELECT '123'::JSONB");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Assert.assertEquals("123", rs.getString(1));
    }

    @Test
    public void testInsertAndSelectJSON() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE x (j JSON)");
        stmt.executeUpdate();
        stmt = conn.prepareStatement("INSERT INTO x VALUES (?)");
        stmt.setString(1, "{\"a\":\"b\"}");
        stmt.executeUpdate();
        stmt = conn.prepareStatement("SELECT j FROM x");
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Assert.assertEquals("{\"a\": \"b\"}", rs.getString(1));
    }
}
