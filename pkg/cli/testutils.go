// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// TestingReset resets global mutable state so that Run can be called multiple
// times from the same test process. It is public for cliccl.
func TestingReset() {
	// Reset the client contexts for each test.
	initCLIDefaults()
}

// TestCLI wraps a test server and is used by tests to make assertions about the output of CLI commands.
type TestCLI struct {
	*server.TestServer
	tenant      serverutils.TestTenantInterface
	certsDir    string
	cleanupFunc func() error
	prevStderr  *os.File

	// t is the testing.T instance used for this test.
	// Example_xxx tests may have this set to nil.
	t *testing.T
	// logScope binds the lifetime of the log files to this test, when t
	// is not nil.
	logScope *log.TestLogScope
	// if true, doesn't print args during RunWithArgs.
	omitArgs bool
	// if true, prints the requested exit code during RunWithArgs.
	reportExitCode bool
}

// TestCLIParams contains parameters used by TestCLI.
type TestCLIParams struct {
	T        *testing.T
	Insecure bool
	// NoServer, if true, starts the test without a DB server.
	NoServer bool

	// The store specifications for the in-memory server.
	StoreSpecs []base.StoreSpec

	// The locality tiers for the in-memory server.
	Locality roachpb.Locality

	// NoNodelocal, if true, disables node-local external I/O storage.
	NoNodelocal bool

	// TenantArgs will be used to initialize the test tenant. This should
	// be set when the test needs to run in multitenant mode.
	TenantArgs *base.TestTenantArgs
}

// testTempFilePrefix is a sentinel marker to be used as the prefix of a
// test file name. It is used to extract the file name from a uniquely
// generated (temp directory) file path.
const testTempFilePrefix = "test-temp-prefix-"

// testUserfileUploadTempDirPrefix is a marker to be used as a prefix for the
// temp directory created in the Example_userfile_upload_recursive() test.
// It is used to extract the filepath.Base(), i.e. the directory name,
// from the uniquely generated (temp directory) file path.
const testUserfileUploadTempDirPrefix = "test-userfile-upload-temp-dir-"

func (c *TestCLI) fail(err interface{}) {
	if c.t != nil {
		defer c.logScope.Close(c.t)
		c.t.Fatal(err)
	} else {
		panic(err)
	}
}

// NewCLITest export for cclcli.
func NewCLITest(params TestCLIParams) TestCLI {
	return newCLITestWithArgs(params, nil)
}

func newCLITestWithArgs(params TestCLIParams, argsFn func(args *base.TestServerArgs)) TestCLI {
	c := TestCLI{t: params.T}

	certsDir, err := ioutil.TempDir("", "cli-test")
	if err != nil {
		c.fail(err)
	}
	c.certsDir = certsDir

	if c.t != nil {
		c.logScope = log.Scope(c.t)
	}

	c.cleanupFunc = func() error { return nil }

	if !params.NoServer {
		if !params.Insecure {
			c.cleanupFunc = securitytest.CreateTestCerts(certsDir)
		}

		args := base.TestServerArgs{
			Insecure:      params.Insecure,
			SSLCertsDir:   c.certsDir,
			StoreSpecs:    params.StoreSpecs,
			Locality:      params.Locality,
			ExternalIODir: filepath.Join(certsDir, "extern"),
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					AOSTClause: "AS OF SYSTEM TIME '-1us'",
				},
			},
		}
		if argsFn != nil {
			argsFn(&args)
		}
		if params.NoNodelocal {
			args.ExternalIODir = ""
		}
		s, err := serverutils.StartServerRaw(args)
		if err != nil {
			c.fail(err)
		}
		c.TestServer = s.(*server.TestServer)

		log.Infof(context.Background(), "server started at %s", c.ServingRPCAddr())
		log.Infof(context.Background(), "SQL listener at %s", c.ServingSQLAddr())
	}

	if params.TenantArgs != nil {
		if c.TestServer == nil {
			c.fail(errors.AssertionFailedf("multitenant mode for CLI requires a DB server, try setting `NoServer` argument to false"))
		}
		if c.Insecure() {
			params.TenantArgs.ForceInsecure = true
		}
		c.tenant, _ = serverutils.StartTenant(c.t, c.TestServer, *params.TenantArgs)
	}
	baseCfg.User = security.NodeUserName()

	// Ensure that CLI error messages and anything meant for the
	// original stderr is redirected to stdout, where it can be
	// captured.
	c.prevStderr = stderr
	stderr = os.Stdout

	return c
}

// setCLIDefaultsForTests invokes initCLIDefaults but pretends the
// output is not a terminal, even if it happens to be. This ensures
// e.g. that tests ran with -v have the same output as those without.
func setCLIDefaultsForTests() {
	initCLIDefaults()
	sqlExecCtx.TerminalOutput = false
	sqlExecCtx.ShowTimes = false
	// Even though we pretend there is no terminal, most tests want
	// pretty tables.
	sqlExecCtx.TableDisplayFormat = clisqlexec.TableDisplayTable
}

// stopServer stops the test server.
func (c *TestCLI) stopServer() {
	if c.TestServer != nil {
		log.Infof(context.Background(), "stopping server at %s / %s",
			c.ServingRPCAddr(), c.ServingSQLAddr())
		c.Stopper().Stop(context.Background())
	}
}

// RestartServer stops and restarts the test server. The ServingRPCAddr() may
// have changed after this method returns.
func (c *TestCLI) RestartServer(params TestCLIParams) {
	c.stopServer()
	log.Info(context.Background(), "restarting server")
	s, err := serverutils.StartServerRaw(base.TestServerArgs{
		Insecure:    params.Insecure,
		SSLCertsDir: c.certsDir,
		StoreSpecs:  params.StoreSpecs,
	})
	if err != nil {
		c.fail(err)
	}
	c.TestServer = s.(*server.TestServer)
	log.Infof(context.Background(), "restarted server at %s / %s",
		c.ServingRPCAddr(), c.ServingSQLAddr())
	if params.TenantArgs != nil {
		if c.Insecure() {
			params.TenantArgs.ForceInsecure = true
		}
		c.tenant, _ = serverutils.StartTenant(c.t, c.TestServer, *params.TenantArgs)
		log.Infof(context.Background(), "restarted tenant SQL only server at %s", c.tenant.SQLAddr())
	}
}

// Cleanup cleans up after the test, stopping the server if necessary.
// The log files are removed if the test has succeeded.
func (c *TestCLI) Cleanup() {
	defer func() {
		if c.t != nil {
			c.logScope.Close(c.t)
		}
	}()

	// Restore stderr.
	stderr = c.prevStderr

	log.Info(context.Background(), "stopping server and cleaning up CLI test")

	c.stopServer()

	if err := c.cleanupFunc(); err != nil {
		panic(err)
	}
}

// Run line of commands.
func (c TestCLI) Run(line string) {
	a := strings.Fields(line)
	c.RunWithArgs(a)
}

// RunWithCapture runs c and returns a string containing the output of c
// and any error that may have occurred capturing the output. We do not propagate
// errors in executing c, because those will be caught when the test verifies
// the output of c.
func (c TestCLI) RunWithCapture(line string) (out string, err error) {
	return captureOutput(func() {
		c.Run(line)
	})
}

// RunWithCaptureArgs args version of RunWithCapture.
func (c TestCLI) RunWithCaptureArgs(args []string) (string, error) {
	return captureOutput(func() {
		c.RunWithArgs(args)
	})
}

// captureOutput runs f and returns a string containing the output and any
// error that may have occurred capturing the output.
func captureOutput(f func()) (out string, err error) {
	// Heavily inspired by Go's testing/example.go:runExample().

	// Funnel stdout into a pipe.
	stdoutSave, stderrRedirSave := os.Stdout, stderr
	r, w, err := os.Pipe()
	if err != nil {
		return "", err
	}
	os.Stdout = w
	stderr = w

	// Send all bytes from piped stdout through the output channel.
	type captureResult struct {
		out string
		err error
	}
	outC := make(chan captureResult)
	go func() {
		var buf bytes.Buffer
		_, err := io.Copy(&buf, r)
		r.Close()
		outC <- captureResult{buf.String(), err}
	}()

	// Clean up and record output in separate function to handle panics.
	defer func() {
		// Close pipe and restore normal stdout.
		w.Close()
		os.Stdout = stdoutSave
		stderr = stderrRedirSave
		outResult := <-outC
		out, err = outResult.out, outResult.err
		if x := recover(); x != nil {
			err = errors.Errorf("panic: %v", x)
		}
	}()

	// Run the command. The output will be returned in the defer block.
	f()
	return
}

func isSQLCommand(args []string) (bool, error) {
	cmd, _, err := cockroachCmd.Find(args)
	if err != nil {
		return false, err
	}
	// We use --echo-sql as a marker of SQL-only commands.
	if f := flagSetForCmd(cmd).Lookup(cliflags.EchoSQL.Name); f != nil {
		return true, nil
	}
	return false, nil
}

func (c TestCLI) getRPCAddr() string {
	if c.tenant != nil {
		return c.tenant.RPCAddr()
	}
	return c.ServingRPCAddr()
}

func (c TestCLI) getSQLAddr() string {
	if c.tenant != nil {
		return c.tenant.SQLAddr()
	}
	return c.ServingSQLAddr()
}

// RunWithArgs add args according to TestCLI cfg.
func (c TestCLI) RunWithArgs(origArgs []string) {
	TestingReset()

	if err := func() error {
		args := append([]string(nil), origArgs[:1]...)
		if c.TestServer != nil {
			addr := c.getRPCAddr()
			if isSQL, err := isSQLCommand(origArgs); err != nil {
				return err
			} else if isSQL {
				addr = c.getSQLAddr()
			}
			h, p, err := net.SplitHostPort(addr)
			if err != nil {
				return err
			}
			args = append(args, fmt.Sprintf("--host=%s", net.JoinHostPort(h, p)))
			if c.Cfg.Insecure {
				args = append(args, "--insecure=true")
			} else {
				args = append(args, "--insecure=false")
				args = append(args, fmt.Sprintf("--certs-dir=%s", c.certsDir))
			}
		}

		args = append(args, origArgs[1:]...)

		// `nodelocal upload` and `userfile upload -r` CLI tests create unique temp
		// directories with random numbers in their names. Given that the expected
		// output for such tests is defined as a static comment, it is not possible
		// to match against the full path. So, we trim the paths as below.
		if len(origArgs) >= 3 && strings.Contains(origArgs[2], testTempFilePrefix) {
			splitFilePath := strings.Split(origArgs[2], testTempFilePrefix)
			origArgs[2] = splitFilePath[1]
		}
		if len(origArgs) >= 4 && strings.Contains(origArgs[3], testUserfileUploadTempDirPrefix) {
			hasTrailingSlash := strings.HasSuffix(origArgs[3], "/")
			origArgs[3] = filepath.Base(origArgs[3])
			// Maintain trailing slash because the behavior of `userfile upload -r`
			// depends on it.
			if hasTrailingSlash {
				origArgs[3] += "/"
			}
		}

		if !c.omitArgs {
			fmt.Fprintf(os.Stderr, "%s\n", args)
			fmt.Println(strings.Join(origArgs, " "))
		}

		return Run(args)
	}(); err != nil {
		clierror.OutputError(os.Stdout, err, true /*showSeverity*/, false /*verbose*/)
		if c.reportExitCode {
			fmt.Fprintln(os.Stdout, "exit code:", getExitCode(err))
		}
	} else {
		if c.reportExitCode {
			fmt.Fprintln(os.Stdout, "exit code:", exit.Success())
		}
	}
}

// RunWithCAArgs adds ca args at run time.
func (c TestCLI) RunWithCAArgs(origArgs []string) {
	TestingReset()

	if err := func() error {
		args := append([]string(nil), origArgs[:1]...)
		if c.TestServer != nil {
			args = append(args, fmt.Sprintf("--ca-key=%s", filepath.Join(c.certsDir, security.EmbeddedCAKey)))
			args = append(args, fmt.Sprintf("--certs-dir=%s", c.certsDir))
		}
		args = append(args, origArgs[1:]...)

		fmt.Fprintf(os.Stderr, "%s\n", args)
		fmt.Println(strings.Join(origArgs, " "))

		return Run(args)
	}(); err != nil {
		fmt.Println(err)
	}
}

// ElideInsecureDeprecationNotice elides the deprecation notice for --insecure.
func ElideInsecureDeprecationNotice(csvStr string) string {
	// v20.1 introduces a deprecation notice for --insecure. Skip over it.
	// TODO(knz): Remove this when --insecure is dropped.
	// See: https://github.com/cockroachdb/cockroach/issues/53404
	lines := strings.SplitN(csvStr, "\n", 3)
	if len(lines) > 0 && strings.HasPrefix(lines[0], "Flag --insecure has been deprecated") {
		csvStr = lines[2]
	}
	return csvStr
}

// RemoveMatchingLines removes lines from the input string that match any of
// the provided regexps. Mind that regexp could match a substrings, so you need
// to put ^ and $ around to ensure full matches.
func RemoveMatchingLines(output string, regexps []string) string {
	if len(regexps) == 0 {
		return output
	}

	var patterns []*regexp.Regexp
	for _, weed := range regexps {
		p := regexp.MustCompile(weed)
		patterns = append(patterns, p)
	}
	filter := func(line string) bool {
		for _, pattern := range patterns {
			if pattern.MatchString(line) {
				return true
			}
		}
		return false
	}

	result := strings.Builder{}
	for _, line := range strings.Split(output, "\n") {
		if filter(line) || len(line) == 0 {
			continue
		}
		result.WriteString(line)
		result.WriteRune('\n')
	}
	return result.String()
}

// GetCsvNumCols returns the number of columns in the given csv string.
func GetCsvNumCols(csvStr string) (cols int, err error) {
	csvStr = ElideInsecureDeprecationNotice(csvStr)
	reader := csv.NewReader(strings.NewReader(csvStr))
	records, err := reader.Read()
	if err != nil {
		return 0, errors.Wrapf(err, "error reading csv input:\n %v\n", csvStr)
	}
	return len(records), nil
}

// MatchCSV matches a multi-line csv string with the provided regex
// (matchColRow[i][j] will be matched against the i-th line, j-th column).
func MatchCSV(csvStr string, matchColRow [][]string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "csv input:\n%v\nexpected:\n%s\n",
				csvStr, pretty.Sprint(matchColRow))
		}
	}()

	csvStr = ElideInsecureDeprecationNotice(csvStr)
	reader := csv.NewReader(strings.NewReader(csvStr))
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	lr, lm := len(records), len(matchColRow)
	if lr < lm {
		return errors.Errorf("csv has %d rows, but expected at least %d", lr, lm)
	}

	// Compare only the last len(matchColRow) records. That is, if we want to
	// match 4 rows and we have 100 records, we only really compare
	// records[96:], that is, the last four rows.
	records = records[lr-lm:]

	for i := range records {
		if lr, lm := len(records[i]), len(matchColRow[i]); lr != lm {
			return errors.Errorf("row #%d: csv has %d columns, but expected %d", i+1, lr, lm)
		}
		for j := range records[i] {
			pat, str := matchColRow[i][j], records[i][j]
			re := regexp.MustCompile(pat)
			if !re.MatchString(str) {
				err = errors.Wrapf(err, "row #%d, col #%d: found %q which does not match %q",
					i+1, j+1, str, pat)
			}
		}
	}
	return err
}
