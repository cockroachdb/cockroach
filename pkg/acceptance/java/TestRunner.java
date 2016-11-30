import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/*
This class compiles and runs all java files under a directory.
Usage : java TestRunner [expected] [dir]
        expected : can be 'SUCCESS' or 'FAIL' depicting the success/failing of a test case
        dir : the directory containing all java test files
 */
public class TestRunner {

    private enum ExpectedOutcome {
        SUCCESS,
        FAIL;

        public static ExpectedOutcome parse(String s) {
            try {
                return ExpectedOutcome.valueOf(s);
            } catch (Exception e) {
                return null;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new Exception("unexpected : missing arguments. usage : java TestRunner [SUCCESS/FAIL] [dir]");
        }

        String mode = args[0], testDirectory = args[1];
        ExpectedOutcome expectedOutcome = ExpectedOutcome.parse(mode);
        if (expectedOutcome == null) {
            throw new Exception("unexpected : missing arguments. usage : java TestRunner [SUCCESS/FAIL] [dir]");
        }

        Result result = runTests(expectedOutcome, testDirectory);
        // Clean up all generated files before throwing the exception
        cleanup();
        if (!result.isSuccess()) {
            throw new Exception(result.getException().getMessage());
        }
    }

    // Runs all test in the {testsDirectory} and checks that each test has the expected outcome
    private static Result runTests(ExpectedOutcome expected, String testsDirectory) throws Exception {
        List<File> tests = listJavaFiles(testsDirectory);
        for (File test : tests) {
            Result testResult = runTest(test);

            switch (expected) {
                case SUCCESS:
                    if (testResult.isSuccess()) {
                        break;
                    }
                    // This test should have "PASSED" but "FAILED
                    return failureResult(
                        String.format("\n\nTest [%s]\nResult : [FAILED]\nExcepted: [SUCCESS]\nCause: %s\n",
                            test.getName(), testResult.getException().getMessage()));

                case FAIL:
                    if (!testResult.isSuccess()) {
                        break;
                    }
                    // This test should have "FAILED" but "PASSED".
                    return failureResult
                        (String.format("\n\nTest [%s]\nResult: [PASSED]\nExcepted: [FAILURE].",
                            test.getName()));
            }
        }
        return successResult();
    }

    // Lists all java files in a directory
    private static List<File> listJavaFiles(String path) {
        List<File> javaFiles = new ArrayList<>();

        File directory = new File(path);
        File[] files = directory.listFiles();
        if (files == null) {
            return javaFiles;
        }

        for (File file : files) {
            if (!file.getName().contains(".java")) {
                continue;
            }
            javaFiles.add(file);
        }
        return javaFiles;
    }

    // Compiles and runs the test java file and returns the result
    private static Result runTest(File test) {
        String javaFile = test.getName();
        String javaFilePrefix = javaFile.substring(0, javaFile.lastIndexOf("."));
        File parent = test.getParentFile();
        String classPath = String.format("/postgres.jar:%s:.", parent.getAbsolutePath());

        String[] compileCmds = new String[] {"javac", test.getAbsolutePath()};
        String[] runCmds = new String[] {"java", "-cp", classPath, javaFilePrefix};

        try {
            // Compiles the java code
            Process compileTest = Runtime.getRuntime().exec(compileCmds);
            String errorTrace = toString(compileTest.getErrorStream());
            compileTest.waitFor();
            if (compileTest.exitValue() != 0) {
                return failureResult(errorTrace);
            }

            // Runs the java code
            Process runTest = Runtime.getRuntime().exec(runCmds);
            errorTrace = toString(runTest.getErrorStream());
            runTest.waitFor();
            if (runTest.exitValue() != 0) {
                return failureResult(errorTrace);
            }

            return successResult();
        } catch (Exception e) {
            return failureResult(e.getMessage());
        }
    }

    // Runs the clean up script
    private static void cleanup() {
        try {
            run(new String[]{"/bin/bash", "-c", "./run_cleanup.sh"});
        } catch (Exception e) {
        }
    }

    private static Process run(String[] cmds) throws Exception {
        Process exec = Runtime.getRuntime().exec(cmds);
        exec.waitFor();
        return exec;
    }

    private static String toString(InputStream ins) throws Exception {
        StringBuilder sb = new StringBuilder();
        String line;
        BufferedReader in = new BufferedReader(new InputStreamReader(ins));
        while ((line = in.readLine()) != null) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    private static Result failureResult(String message) {
        return new Result(false, new Exception(message));
    }

    private static Result successResult() {
        return new Result(true, null);
    }

    // A wrapper object containing test case execution result and the exception
    private static class Result {
        private final boolean success;
        private final Exception exception;

        public Result(boolean success, Exception exception) {
            this.success = success;
            this.exception = exception;
        }

        public boolean isSuccess() {
            return success;
        }

        public Exception getException() {
            return exception;
        }
    }
}