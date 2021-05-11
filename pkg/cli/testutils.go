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
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
}

// TestCLIParams contains parameters used by TestCLI.
type TestCLIParams struct {
	T           *testing.T
	Insecure    bool
	NoServer    bool
	StoreSpecs  []base.StoreSpec
	Locality    roachpb.Locality
	NoNodelocal bool
}

// testTempFilePrefix is a sentinel marker to be used as the prefix of a
// test file name. It is used to extract the file name from a uniquely
// generated (temp directory) file path.
const testTempFilePrefix = "test-temp-prefix-"

func (c *TestCLI) fail(err interface{}) {
	if c.t != nil {
		defer c.logScope.Close(c.t)
		c.t.Fatal(err)
	} else {
		panic(err)
	}
}

func createTestCerts(certsDir string) (cleanup func() error) {
	// Copy these assets to disk from embedded strings, so this test can
	// run from a standalone binary.
	// Disable embedded certs, or the security library will try to load
	// our real files as embedded assets.
	security.ResetAssetLoader()

	assets := []string{
		filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert),
		filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCAKey),
		filepath.Join(security.EmbeddedCertsDir, security.EmbeddedNodeCert),
		filepath.Join(security.EmbeddedCertsDir, security.EmbeddedNodeKey),
		filepath.Join(security.EmbeddedCertsDir, security.EmbeddedRootCert),
		filepath.Join(security.EmbeddedCertsDir, security.EmbeddedRootKey),
		filepath.Join(security.EmbeddedCertsDir, security.EmbeddedTenantClientCACert),
	}

	for _, a := range assets {
		_, err := securitytest.RestrictedCopy(a, certsDir, filepath.Base(a))
		if err != nil {
			panic(err)
		}
	}

	return func() error {
		security.SetAssetLoader(securitytest.EmbeddedAssets)
		return os.RemoveAll(certsDir)
	}
}

// NewCLITest export for cclcli.
func NewCLITest(params TestCLIParams) TestCLI {
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
			c.cleanupFunc = createTestCerts(certsDir)
		}

		args := base.TestServerArgs{
			Insecure:      params.Insecure,
			SSLCertsDir:   c.certsDir,
			StoreSpecs:    params.StoreSpecs,
			Locality:      params.Locality,
			ExternalIODir: filepath.Join(certsDir, "extern"),
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
	cliCtx.terminalOutput = false
	sqlCtx.showTimes = false
	// Even though we pretend there is no terminal, most tests want
	// pretty tables.
	cliCtx.tableDisplayFormat = tableDisplayTable
}

// stopServer stops the test server.
func (c *TestCLI) stopServer() {
	if c.TestServer != nil {
		log.Infof(context.Background(), "stopping server at %s / %s",
			c.ServingRPCAddr(), c.ServingSQLAddr())
		c.Stopper().Stop(context.Background())
	}
}

// restartServer stops and restarts the test server. The ServingRPCAddr() may
// have changed after this method returns.
func (c *TestCLI) restartServer(params TestCLIParams) {
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

// RunWithArgs add args according to TestCLI cfg.
func (c TestCLI) RunWithArgs(origArgs []string) {
	TestingReset()

	if err := func() error {
		args := append([]string(nil), origArgs[:1]...)
		if c.TestServer != nil {
			addr := c.ServingRPCAddr()
			if isSQL, err := isSQLCommand(origArgs); err != nil {
				return err
			} else if isSQL {
				addr = c.ServingSQLAddr()
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

		// `nodelocal upload` CLI tests create test files in unique temp
		// directories. Given that the expected output for such tests is defined as
		// a static comment, it is not possible to match against the full file path.
		// So, we trim the file path upto the sentinel prefix marker, and use only
		// the file name for comparing against the expected output.
		if len(origArgs) >= 3 && strings.Contains(origArgs[2], testTempFilePrefix) {
			splitFilePath := strings.Split(origArgs[2], testTempFilePrefix)
			origArgs[2] = splitFilePath[1]
		}

		if !c.omitArgs {
			fmt.Fprintf(os.Stderr, "%s\n", args)
			fmt.Println(strings.Join(origArgs, " "))
		}

		return Run(args)
	}(); err != nil {
		cliOutputError(os.Stdout, err, true /*showSeverity*/, false /*verbose*/)
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
