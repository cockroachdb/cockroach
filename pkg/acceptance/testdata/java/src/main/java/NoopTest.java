import org.junit.Test;

public class NoopTest {
    @Test
    public void testNoop() throws Exception {
      // This is a test we can target when building the Docker image to ensure
      // Maven downloads all packages necessary for testing before we enter
      // offline mode. (Maven dependency:go-offline is not bulletproof, and
      // leaves out surefire-junit.)
    }
}
