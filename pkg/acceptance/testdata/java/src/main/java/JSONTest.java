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
}
