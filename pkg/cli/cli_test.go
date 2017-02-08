// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package cli

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/cobra"
)

type cliTest struct {
	*server.TestServer
	certsDir    string
	cleanupFunc func() error

	// t is the testing.T instance used for this test.
	// Example_xxx tests may have this set to nil.
	t *testing.T
	// logScope binds the lifetime of the log files to this test, when t
	// is not nil
	logScope log.TestLogScope
}

type cliTestParams struct {
	t        *testing.T
	insecure bool
	noServer bool
}

func newCLITest(params cliTestParams) cliTest {
	c := cliTest{t: params.t}

	fail := func(err interface{}) {
		if c.t != nil {
			defer c.logScope.Close(c.t)
			c.t.Fatal(err)
		} else {
			panic(err)
		}
	}

	certsDir, err := ioutil.TempDir("", "cli-test")
	if err != nil {
		fail(err)
	}
	c.certsDir = certsDir

	// Reset the client context for each test. We don't reset the
	// pointer (because they are tied into the flags), but instead
	// overwrite the existing struct's values.
	baseCfg.InitDefaults()
	InitCLIDefaults()

	if c.t != nil {
		c.logScope = log.Scope(c.t, "")
	}

	c.cleanupFunc = func() error { return nil }

	if !params.noServer {
		s, err := serverutils.StartServerRaw(base.TestServerArgs{Insecure: params.insecure})
		if err != nil {
			fail(err)
		}
		c.TestServer = s.(*server.TestServer)

		if !params.insecure {
			// Copy these assets to disk from embedded strings, so this test can
			// run from a standalone binary.
			// Disable embedded certs, or the security library will try to load
			// our real files as embedded assets.
			security.ResetReadFileFn()

			assets := []string{
				filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert),
				filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCAKey),
				filepath.Join(security.EmbeddedCertsDir, security.EmbeddedNodeCert),
				filepath.Join(security.EmbeddedCertsDir, security.EmbeddedNodeKey),
				filepath.Join(security.EmbeddedCertsDir, security.EmbeddedRootCert),
				filepath.Join(security.EmbeddedCertsDir, security.EmbeddedRootKey),
			}

			for _, a := range assets {
				securitytest.RestrictedCopy(nil, a, certsDir, filepath.Base(a))
			}

			c.cleanupFunc = func() error {
				security.SetReadFileFn(securitytest.Asset)
				return os.RemoveAll(certsDir)
			}
		}
	}

	// Ensure that CLI error messages are logged to stdout, where they
	// can be captured.
	osStderr = os.Stdout

	return c
}

// cleanup cleans up after the test, stopping the server if necessary.
// The log files are removed if the test has succeeded.
func (c cliTest) cleanup() {
	if c.t != nil {
		defer c.logScope.Close(c.t)
	}

	// Restore stderr.
	osStderr = os.Stderr

	if c.TestServer != nil {
		select {
		case <-c.Stopper().ShouldStop():
			// If ShouldStop() doesn't block, that means someone has already
			// called Stop(). We just need to wait.
			<-c.Stopper().IsStopped()
		default:
			c.Stopper().Stop()
		}
	}

	if err := c.cleanupFunc(); err != nil {
		panic(err)
	}
}

func (c cliTest) Run(line string) {
	a := strings.Fields(line)
	c.RunWithArgs(a)
}

// RunWithCapture runs c and returns a string containing the output of c
// and any error that may have occurred capturing the output. We do not propagate
// errors in executing c, because those will be caught when the test verifies
// the output of c.
func (c cliTest) RunWithCapture(line string) (out string, err error) {
	return captureOutput(func() {
		c.Run(line)
	})
}

// captureOutput runs f and returns a string containing the output and any
// error that may have occurred capturing the output.
func captureOutput(f func()) (out string, err error) {
	// Heavily inspired by Go's testing/example.go:runExample().

	// Funnel stdout into a pipe.
	stdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		return "", err
	}
	os.Stdout = w

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
		os.Stdout = stdout
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

func (c cliTest) RunWithArgs(origArgs []string) {
	sqlCtx.execStmts = nil
	zoneConfig = ""
	zoneDisableReplication = false

	if err := func() error {
		args := append([]string(nil), origArgs[:1]...)
		if c.TestServer != nil {
			h, p, err := net.SplitHostPort(c.ServingAddr())
			if err != nil {
				return err
			}
			if c.Cfg.Insecure {
				args = append(args, "--insecure")
			} else {
				args = append(args, "--insecure=false")
				args = append(args, fmt.Sprintf("--ca-cert=%s", filepath.Join(c.certsDir, security.EmbeddedCACert)))
				args = append(args, fmt.Sprintf("--cert=%s", filepath.Join(c.certsDir, security.EmbeddedNodeCert)))
				args = append(args, fmt.Sprintf("--key=%s", filepath.Join(c.certsDir, security.EmbeddedNodeKey)))
			}
			args = append(args, fmt.Sprintf("--host=%s", h))
			args = append(args, fmt.Sprintf("--port=%s", p))
		}
		args = append(args, origArgs[1:]...)

		fmt.Fprintf(os.Stderr, "%s\n", args)
		fmt.Println(strings.Join(origArgs, " "))

		return Run(args)
	}(); err != nil {
		fmt.Println(err)
	}
}

func TestQuit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testing.Short() {
		t.Skip("short flag")
	}

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	c.Run("quit")
	// Wait until this async command cleanups the server.
	<-c.Stopper().IsStopped()

	// NB: if this test is ever flaky due to port reuse, we could run against
	// :0 (which however changes some of the errors we get).
	// One way of getting that is:
	//	c.Cfg.AdvertiseAddr = "127.0.0.1:0"

	styled := func(s string) string {
		const preamble = `unable to connect or connection lost.

Please check the address and credentials such as certificates \(if attempting to
communicate with a secure cluster\).

`
		return preamble + s
	}

	for i, test := range []struct {
		cmd, expOutPattern string
	}{
		// Error returned from GRPC to internal/client (which has to pass it
		// up the stack as a roachpb.NewError(roachpb.NewSendError(.)).
		{`debug kv inc a`, styled(
			`increment failed: failed to send RPC: rpc error: code = 14 desc = grpc: the connection is unavailable`),
		},
		// Error returned directly from GRPC.
		{`quit`, styled(
			`rpc error: code = 14 desc = grpc: the connection is unavailable`),
		},
		// Going through the SQL client libraries gives a *net.OpError which
		// we also handle.
		{`zone ls`, styled(
			`dial tcp .*: getsockopt: connection refused`),
		},
		// A real error before we hit any networking ones.
		{`debug kv scan b a`, `start key must be smaller than end key`},
		{`debug kv inc`, `invalid arguments`},
	} {
		out, err := c.RunWithCapture(test.cmd)
		if err != nil {
			t.Fatal(errors.Wrap(err, strconv.Itoa(i)))
		}
		exp := test.cmd + "\n" + test.expOutPattern
		re := regexp.MustCompile(exp)
		if !re.MatchString(out) {
			t.Errorf("expected '%s' to match pattern\n%s\ngot:\n%s", test.cmd, exp, out)
		}
	}
}

func Example_basic() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	c.Run("debug kv put a 1 b 2 c 3")
	c.Run("debug kv scan")
	c.Run("debug kv revscan")
	c.Run("debug kv del a c")
	c.Run("debug kv get a")
	c.Run("debug kv get b")
	c.Run("debug kv inc c 1")
	c.Run("debug kv inc c 10")
	c.Run("debug kv inc c 100")
	c.Run("debug kv inc c -- -60")
	c.Run("debug kv inc c -- -9")
	c.Run("debug kv scan")
	c.Run("debug kv revscan")
	c.Run("debug kv inc c b")

	// Output:
	// debug kv put a 1 b 2 c 3
	// debug kv scan
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// 3 result(s)
	// debug kv revscan
	// "c"	"3"
	// "b"	"2"
	// "a"	"1"
	// 3 result(s)
	// debug kv del a c
	// debug kv get a
	// "a" not found
	// debug kv get b
	// "2"
	// debug kv inc c 1
	// 1
	// debug kv inc c 10
	// 11
	// debug kv inc c 100
	// 111
	// debug kv inc c -- -60
	// 51
	// debug kv inc c -- -9
	// 42
	// debug kv scan
	// "b"	"2"
	// "c"	42
	// 2 result(s)
	// debug kv revscan
	// "c"	42
	// "b"	"2"
	// 2 result(s)
	// debug kv inc c b
	// invalid increment: strconv.ParseInt: parsing "b": invalid syntax
}

func Example_quoted() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	c.Run(`debug kv put a\x00 日本語`)                                  // UTF-8 input text
	c.Run(`debug kv put a\x01 \u65e5\u672c\u8a9e`)                   // explicit Unicode code points
	c.Run(`debug kv put a\x02 \U000065e5\U0000672c\U00008a9e`)       // explicit Unicode code points
	c.Run(`debug kv put a\x03 \xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e`) // explicit UTF-8 bytes
	c.Run(`debug kv scan`)
	c.Run(`debug kv get a\x00`)
	c.Run(`debug kv del a\x00`)
	c.Run(`debug kv inc 1\x01`)
	c.Run(`debug kv get 1\x01`)

	// Output:
	// debug kv put a\x00 日本語
	// debug kv put a\x01 \u65e5\u672c\u8a9e
	// debug kv put a\x02 \U000065e5\U0000672c\U00008a9e
	// debug kv put a\x03 \xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e
	// debug kv scan
	// "a\x00"	"日本語"
	// "a\x01"	"日本語"
	// "a\x02"	"日本語"
	// "a\x03"	"日本語"
	// 4 result(s)
	// debug kv get a\x00
	// "日本語"
	// debug kv del a\x00
	// debug kv inc 1\x01
	// 1
	// debug kv get 1\x01
	// 1
}

func Example_insecure() {
	c := newCLITest(cliTestParams{insecure: true})
	defer c.cleanup()

	c.Run("debug kv put a 1 b 2")
	c.Run("debug kv scan")

	// Output:
	// debug kv put a 1 b 2
	// debug kv scan
	// "a"	"1"
	// "b"	"2"
	// 2 result(s)
}

func Example_ranges() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	c.Run("debug kv put a 1 b 2 c 3 d 4")
	c.Run("debug kv scan")
	c.Run("debug kv revscan")
	c.Run("debug range split c")
	c.Run("debug range ls")
	c.Run("debug kv scan")
	c.Run("debug kv revscan")
	c.Run("debug kv delrange a c")
	c.Run("debug kv scan")

	// Output:
	// debug kv put a 1 b 2 c 3 d 4
	// debug kv scan
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// "d"	"4"
	// 4 result(s)
	// debug kv revscan
	// "d"	"4"
	// "c"	"3"
	// "b"	"2"
	// "a"	"1"
	// 4 result(s)
	// debug range split c
	// debug range ls
	// /Min-"c" [1]
	// 	0: node-id=1 store-id=1
	// "c"-/Table/0 [7]
	// 	0: node-id=1 store-id=1
	// /Table/0-/Table/11 [2]
	// 	0: node-id=1 store-id=1
	// /Table/11-/Table/12 [3]
	// 	0: node-id=1 store-id=1
	// /Table/12-/Table/13 [4]
	//	0: node-id=1 store-id=1
	// /Table/13-/Table/14 [5]
	//	0: node-id=1 store-id=1
	// /Table/14-/Max [6]
	//	0: node-id=1 store-id=1
	// 7 result(s)
	// debug kv scan
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// "d"	"4"
	// 4 result(s)
	// debug kv revscan
	// "d"	"4"
	// "c"	"3"
	// "b"	"2"
	// "a"	"1"
	// 4 result(s)
	// debug kv delrange a c
	// debug kv scan
	// "c"	"3"
	// "d"	"4"
	// 2 result(s)
}

func Example_logging() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	c.RunWithArgs([]string{"sql", "--alsologtostderr=false", "-e", "select 1"})
	c.RunWithArgs([]string{"sql", "--log-backtrace-at=foo.go:1", "-e", "select 1"})
	c.RunWithArgs([]string{"sql", "--log-dir=", "-e", "select 1"})
	c.RunWithArgs([]string{"sql", "--logtostderr=true", "-e", "select 1"})
	c.RunWithArgs([]string{"sql", "--verbosity=0", "-e", "select 1"})
	c.RunWithArgs([]string{"sql", "--vmodule=foo=1", "-e", "select 1"})

	// Output:
	// sql --alsologtostderr=false -e select 1
	// 1 row
	// 1
	// 1
	// sql --log-backtrace-at=foo.go:1 -e select 1
	// 1 row
	// 1
	// 1
	// sql --log-dir= -e select 1
	// 1 row
	// 1
	// 1
	// sql --logtostderr=true -e select 1
	// 1 row
	// 1
	// 1
	// sql --verbosity=0 -e select 1
	// 1 row
	// 1
	// 1
	// sql --vmodule=foo=1 -e select 1
	// 1 row
	// 1
	// 1
}

func Example_cput() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	c.Run("debug kv put a 1 b 2 c 3 d 4")
	c.Run("debug kv scan")
	c.Run("debug kv cput e 5")
	c.Run("debug kv cput b 3 2")
	c.Run("debug kv scan")

	// Output:
	// debug kv put a 1 b 2 c 3 d 4
	// debug kv scan
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// "d"	"4"
	// 4 result(s)
	// debug kv cput e 5
	// debug kv cput b 3 2
	// debug kv scan
	// "a"	"1"
	// "b"	"3"
	// "c"	"3"
	// "d"	"4"
	// "e"	"5"
	// 5 result(s)
}

func Example_max_results() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	c.Run("debug kv put a 1 b 2 c 3 d 4")
	c.Run("debug kv scan --max-results=3")
	c.Run("debug kv revscan --max-results=2")
	c.Run("debug range split c")
	c.Run("debug range split d")
	c.Run("debug range ls --max-results=2")

	// Output:
	// debug kv put a 1 b 2 c 3 d 4
	// debug kv scan --max-results=3
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// 3 result(s)
	// debug kv revscan --max-results=2
	// "d"	"4"
	// "c"	"3"
	// 2 result(s)
	// debug range split c
	// debug range split d
	// debug range ls --max-results=2
	// /Min-"c" [1]
	// 	0: node-id=1 store-id=1
	// "c"-"d" [7]
	// 	0: node-id=1 store-id=1
	// 2 result(s)
}

func Example_zone() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	c.Run("zone ls")
	c.Run("zone set system --file=./testdata/zone_attrs.yaml")
	c.Run("zone ls")
	c.Run("zone get system.nonexistent")
	c.Run("zone get system.lease")
	c.Run("zone set system --file=./testdata/zone_range_max_bytes.yaml")
	c.Run("zone get system")
	c.Run("zone rm system")
	c.Run("zone ls")
	c.Run("zone rm .default")
	c.Run("zone set .default --file=./testdata/zone_range_max_bytes.yaml")
	c.Run("zone get system")
	c.Run("zone set .default --disable-replication")
	c.Run("zone get system")

	// Output:
	// zone ls
	// .default
	// zone set system --file=./testdata/zone_attrs.yaml
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	// gc:
	//   ttlseconds: 86400
	// num_replicas: 1
	// constraints: [us-east-1a, ssd]
	// zone ls
	// .default
	// system
	// zone get system.nonexistent
	// system.nonexistent not found
	// zone get system.lease
	// system
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	// gc:
	//   ttlseconds: 86400
	// num_replicas: 1
	// constraints: [us-east-1a, ssd]
	// zone set system --file=./testdata/zone_range_max_bytes.yaml
	// range_min_bytes: 1048576
	// range_max_bytes: 134217728
	// gc:
	//   ttlseconds: 86400
	// num_replicas: 3
	// constraints: [us-east-1a, ssd]
	// zone get system
	// system
	// range_min_bytes: 1048576
	// range_max_bytes: 134217728
	// gc:
	//   ttlseconds: 86400
	// num_replicas: 3
	// constraints: [us-east-1a, ssd]
	// zone rm system
	// DELETE 1
	// zone ls
	// .default
	// zone rm .default
	// unable to remove .default
	// zone set .default --file=./testdata/zone_range_max_bytes.yaml
	// range_min_bytes: 1048576
	// range_max_bytes: 134217728
	// gc:
	//   ttlseconds: 86400
	// num_replicas: 3
	// constraints: []
	// zone get system
	// .default
	// range_min_bytes: 1048576
	// range_max_bytes: 134217728
	// gc:
	//   ttlseconds: 86400
	// num_replicas: 3
	// constraints: []
	// zone set .default --disable-replication
	// range_min_bytes: 1048576
	// range_max_bytes: 134217728
	// gc:
	//   ttlseconds: 86400
	// num_replicas: 1
	// constraints: []
	// zone get system
	// .default
	// range_min_bytes: 1048576
	// range_max_bytes: 134217728
	// gc:
	//   ttlseconds: 86400
	// num_replicas: 1
	// constraints: []
}

func Example_sql() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	c.RunWithArgs([]string{"sql", "-e", "create database t; create table t.f (x int, y int); insert into t.f values (42, 69)"})
	c.RunWithArgs([]string{"sql", "-e", "select 3", "-e", "select * from t.f"})
	c.RunWithArgs([]string{"sql", "-e", "begin", "-e", "select 3", "-e", "commit"})
	c.RunWithArgs([]string{"sql", "-e", "select * from t.f"})
	c.RunWithArgs([]string{"sql", "--execute=show databases"})
	c.RunWithArgs([]string{"sql", "-e", "select 1; select 2"})

	// Output:
	// sql -e create database t; create table t.f (x int, y int); insert into t.f values (42, 69)
	// INSERT 1
	// sql -e select 3 -e select * from t.f
	// 1 row
	// 3
	// 3
	// 1 row
	// x	y
	// 42	69
	// sql -e begin -e select 3 -e commit
	// BEGIN
	// 1 row
	// 3
	// 3
	// COMMIT
	// sql -e select * from t.f
	// 1 row
	// x	y
	// 42	69
	// sql --execute=show databases
	// 4 rows
	// Database
	// information_schema
	// pg_catalog
	// system
	// t
	// sql -e select 1; select 2
	// 1 row
	// 1
	// 1
	// 1 row
	// 2
	// 2
}

func Example_sql_format() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	c.RunWithArgs([]string{"sql", "-e", "create database t; create table t.times (bare timestamp, withtz timestamptz)"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.times values ('2016-01-25 10:10:10', '2016-01-25 10:10:10-05:00')"})
	c.RunWithArgs([]string{"sql", "-e", "select * from t.times"})

	// Output:
	// sql -e create database t; create table t.times (bare timestamp, withtz timestamptz)
	// CREATE TABLE
	// sql -e insert into t.times values ('2016-01-25 10:10:10', '2016-01-25 10:10:10-05:00')
	// INSERT 1
	// sql -e select * from t.times
	// 1 row
	// bare	withtz
	// 2016-01-25 10:10:10+00:00	2016-01-25 15:10:10+00:00
}

func Example_sql_column_labels() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	c.RunWithArgs([]string{"sql", "-e", "create database t; create table t.u (\"\"\"foo\" int, \"\\foo\" int, \"foo\nbar\" int, \"κόσμε\" int, \"a|b\" int, \"܈85\" int)"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.u values (0, 0, 0, 0, 0, 0)"})
	c.RunWithArgs([]string{"sql", "-e", "show columns from t.u"})
	c.RunWithArgs([]string{"sql", "-e", "select * from t.u"})
	c.RunWithArgs([]string{"sql", "--format=pretty", "-e", "show columns from t.u"})
	c.RunWithArgs([]string{"sql", "--format=pretty", "-e", "select * from t.u"})
	c.RunWithArgs([]string{"sql", "--format=tsv", "-e", "select * from t.u"})
	c.RunWithArgs([]string{"sql", "--format=csv", "-e", "select * from t.u"})
	c.RunWithArgs([]string{"sql", "--format=sql", "-e", "select * from t.u"})
	c.RunWithArgs([]string{"sql", "--format=html", "-e", "select * from t.u"})
	c.RunWithArgs([]string{"sql", "--format=records", "-e", "select * from t.u"})

	// Output:
	// sql -e create database t; create table t.u ("""foo" int, "\foo" int, "foo
	// bar" int, "κόσμε" int, "a|b" int, "܈85" int)
	// CREATE TABLE
	// sql -e insert into t.u values (0, 0, 0, 0, 0, 0)
	// INSERT 1
	// sql -e show columns from t.u
	// 6 rows
	// Field	Type	Null	Default	Indices
	// """foo"	INT	true	NULL	{}
	// \foo	INT	true	NULL	{}
	// "foo
	// bar"	INT	true	NULL	{}
	// κόσμε	INT	true	NULL	{}
	// a|b	INT	true	NULL	{}
	// ܈85	INT	true	NULL	{}
	// sql -e select * from t.u
	// 1 row
	// """foo"	\foo	"""foo\nbar"""	κόσμε	a|b	܈85
	// 0	0	0	0	0	0
	// sql --format=pretty -e show columns from t.u
	// +---------+------+------+---------+---------+
	// |  Field  | Type | Null | Default | Indices |
	// +---------+------+------+---------+---------+
	// | "foo    | INT  | true | NULL    | {}      |
	// | \foo    | INT  | true | NULL    | {}      |
	// | foo␤    | INT  | true | NULL    | {}      |
	// | bar     |      |      |         |         |
	// | κόσμε   | INT  | true | NULL    | {}      |
	// | a|b     | INT  | true | NULL    | {}      |
	// | ܈85     | INT  | true | NULL    | {}      |
	// +---------+------+------+---------+---------+
	// (6 rows)
	// sql --format=pretty -e select * from t.u
	// +------+------+------------+-------+-----+-----+
	// | "foo | \foo | "foo\nbar" | κόσμε | a|b | ܈85 |
	// +------+------+------------+-------+-----+-----+
	// |    0 |    0 |          0 |     0 |   0 |   0 |
	// +------+------+------------+-------+-----+-----+
	// (1 row)
	// sql --format=tsv -e select * from t.u
	// 1 row
	// """foo"	\foo	"""foo\nbar"""	κόσμε	a|b	܈85
	// 0	0	0	0	0	0
	// sql --format=csv -e select * from t.u
	// 1 row
	// """foo",\foo,"""foo\nbar""",κόσμε,a|b,܈85
	// 0,0,0,0,0,0
	// sql --format=sql -e select * from t.u
	// CREATE TABLE results (
	//   """foo" STRING,
	//   "\foo" STRING,
	//   """foo\nbar""" STRING,
	//   κόσμε STRING,
	//   "a|b" STRING,
	//   ܈85 STRING
	// );
	//
	// INSERT INTO results VALUES ('0', '0', '0', '0', '0', '0');
	// sql --format=html -e select * from t.u
	// <table>
	// <thead><tr><th>&#34;foo</th><th>\foo</th><th>&#34;foo\nbar&#34;</th><th>κόσμε</th><th>a|b</th><th>܈85</th></tr></head>
	// <tbody>
	// <tr><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td></tr>
	// </tbody>
	// </table>
	// sql --format=records -e select * from t.u
	// -[ RECORD 1 ]
	// "foo       | 0
	// \foo       | 0
	// "foo\nbar" | 0
	// κόσμε      | 0
	// a|b        | 0
	// ܈85        | 0
}

func Example_sql_table() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	c.RunWithArgs([]string{"sql", "-e", "create database t; create table t.t (s string, d string);"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'foo', 'printable ASCII')"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'\"foo', 'printable ASCII with quotes')"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'\\\\foo', 'printable ASCII with backslash')"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'foo\\x0abar', 'non-printable ASCII')"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values ('κόσμε', 'printable UTF8')"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'\\xc3\\xb1', 'printable UTF8 using escapes')"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'\\x01', 'non-printable UTF8 string')"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'\\xdc\\x88\\x38\\x35', 'UTF8 string with RTL char')"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'\\xc3\\x28', 'non-UTF8 string')"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'a\\tb\\tc\\n12\\t123123213\\t12313', 'tabs')"})
	c.RunWithArgs([]string{"sql", "-e", "select * from t.t"})
	c.RunWithArgs([]string{"sql", "--format=pretty", "-e", "select * from t.t"})
	c.RunWithArgs([]string{"sql", "--format=tsv", "-e", "select * from t.t"})
	c.RunWithArgs([]string{"sql", "--format=csv", "-e", "select * from t.t"})
	c.RunWithArgs([]string{"sql", "--format=sql", "-e", "select * from t.t"})
	c.RunWithArgs([]string{"sql", "--format=html", "-e", "select * from t.t"})
	c.RunWithArgs([]string{"sql", "--format=records", "-e", "select * from t.t"})
	c.RunWithArgs([]string{"sql", "--format=pretty", "-e", "select '  hai' as x"})
	c.RunWithArgs([]string{"sql", "--format=pretty", "-e", "explain(indent) select s from t.t union all select s from t.t"})

	// Output:
	// sql -e create database t; create table t.t (s string, d string);
	// CREATE TABLE
	// sql -e insert into t.t values (e'foo', 'printable ASCII')
	// INSERT 1
	// sql -e insert into t.t values (e'"foo', 'printable ASCII with quotes')
	// INSERT 1
	// sql -e insert into t.t values (e'\\foo', 'printable ASCII with backslash')
	// INSERT 1
	// sql -e insert into t.t values (e'foo\x0abar', 'non-printable ASCII')
	// INSERT 1
	// sql -e insert into t.t values ('κόσμε', 'printable UTF8')
	// INSERT 1
	// sql -e insert into t.t values (e'\xc3\xb1', 'printable UTF8 using escapes')
	// INSERT 1
	// sql -e insert into t.t values (e'\x01', 'non-printable UTF8 string')
	// INSERT 1
	// sql -e insert into t.t values (e'\xdc\x88\x38\x35', 'UTF8 string with RTL char')
	// INSERT 1
	// sql -e insert into t.t values (e'\xc3\x28', 'non-UTF8 string')
	// pq: invalid UTF-8 byte sequence
	// insert into t.t values (e'\xc3\x28', 'non-UTF8 string')
	//                         ^
	//
	// sql -e insert into t.t values (e'a\tb\tc\n12\t123123213\t12313', 'tabs')
	// INSERT 1
	// sql -e select * from t.t
	// 9 rows
	// s	d
	// foo	printable ASCII
	// """foo"	printable ASCII with quotes
	// \foo	printable ASCII with backslash
	// "foo
	// bar"	non-printable ASCII
	// κόσμε	printable UTF8
	// ñ	printable UTF8 using escapes
	// """\x01"""	non-printable UTF8 string
	// ܈85	UTF8 string with RTL char
	// "a	b	c
	// 12	123123213	12313"	tabs
	// sql --format=pretty -e select * from t.t
	// +--------------------------------+--------------------------------+
	// |               s                |               d                |
	// +--------------------------------+--------------------------------+
	// | foo                            | printable ASCII                |
	// | "foo                           | printable ASCII with quotes    |
	// | \foo                           | printable ASCII with backslash |
	// | foo␤                           | non-printable ASCII            |
	// | bar                            |                                |
	// | κόσμε                          | printable UTF8                 |
	// | ñ                              | printable UTF8 using escapes   |
	// | "\x01"                         | non-printable UTF8 string      |
	// | ܈85                            | UTF8 string with RTL char      |
	// | a   b         c␤               | tabs                           |
	// | 12  123123213 12313            |                                |
	// +--------------------------------+--------------------------------+
	// (9 rows)
	// sql --format=tsv -e select * from t.t
	// 9 rows
	// s	d
	// foo	printable ASCII
	// """foo"	printable ASCII with quotes
	// \foo	printable ASCII with backslash
	// "foo
	// bar"	non-printable ASCII
	// κόσμε	printable UTF8
	// ñ	printable UTF8 using escapes
	// """\x01"""	non-printable UTF8 string
	// ܈85	UTF8 string with RTL char
	// "a	b	c
	// 12	123123213	12313"	tabs
	// sql --format=csv -e select * from t.t
	// 9 rows
	// s,d
	// foo,printable ASCII
	// """foo",printable ASCII with quotes
	// \foo,printable ASCII with backslash
	// "foo
	// bar",non-printable ASCII
	// κόσμε,printable UTF8
	// ñ,printable UTF8 using escapes
	// """\x01""",non-printable UTF8 string
	// ܈85,UTF8 string with RTL char
	// "a	b	c
	// 12	123123213	12313",tabs
	// sql --format=sql -e select * from t.t
	// CREATE TABLE results (
	//   s STRING,
	//   d STRING
	// );
	//
	// INSERT INTO results VALUES ('foo', 'printable ASCII');
	// INSERT INTO results VALUES ('"foo', 'printable ASCII with quotes');
	// INSERT INTO results VALUES (e'\\foo', 'printable ASCII with backslash');
	// INSERT INTO results VALUES (e'foo\nbar', 'non-printable ASCII');
	// INSERT INTO results VALUES (e'\u03BA\U00001F79\u03C3\u03BC\u03B5', 'printable UTF8');
	// INSERT INTO results VALUES (e'\u00F1', 'printable UTF8 using escapes');
	// INSERT INTO results VALUES (e'"\\x01"', 'non-printable UTF8 string');
	// INSERT INTO results VALUES (e'\u070885', 'UTF8 string with RTL char');
	// INSERT INTO results VALUES (e'a\tb\tc\n12\t123123213\t12313', 'tabs');
	// sql --format=html -e select * from t.t
	// <table>
	// <thead><tr><th>s</th><th>d</th></tr></head>
	// <tbody>
	// <tr><td>foo</td><td>printable ASCII</td></tr>
	// <tr><td>&#34;foo</td><td>printable ASCII with quotes</td></tr>
	// <tr><td>\foo</td><td>printable ASCII with backslash</td></tr>
	// <tr><td>foo<br/>bar</td><td>non-printable ASCII</td></tr>
	// <tr><td>κόσμε</td><td>printable UTF8</td></tr>
	// <tr><td>ñ</td><td>printable UTF8 using escapes</td></tr>
	// <tr><td>&#34;\x01&#34;</td><td>non-printable UTF8 string</td></tr>
	// <tr><td>܈85</td><td>UTF8 string with RTL char</td></tr>
	// <tr><td>a	b	c<br/>12	123123213	12313</td><td>tabs</td></tr>
	// </tbody>
	// </table>
	// sql --format=records -e select * from t.t
	// -[ RECORD 1 ]
	// s | foo
	// d | printable ASCII
	// -[ RECORD 2 ]
	// s | "foo
	// d | printable ASCII with quotes
	// -[ RECORD 3 ]
	// s | \foo
	// d | printable ASCII with backslash
	// -[ RECORD 4 ]
	// s | foo
	//   | bar
	// d | non-printable ASCII
	// -[ RECORD 5 ]
	// s | κόσμε
	// d | printable UTF8
	// -[ RECORD 6 ]
	// s | ñ
	// d | printable UTF8 using escapes
	// -[ RECORD 7 ]
	// s | "\x01"
	// d | non-printable UTF8 string
	// -[ RECORD 8 ]
	// s | ܈85
	// d | UTF8 string with RTL char
	// -[ RECORD 9 ]
	// s | a	b	c
	//   | 12	123123213	12313
	// d | tabs
	// sql --format=pretty -e select '  hai' as x
	// +-------+
	// |   x   |
	// +-------+
	// |   hai |
	// +-------+
	// (1 row)
	// sql --format=pretty -e explain(indent) select s from t.t union all select s from t.t
	// +-------+--------+-------+--------------------+
	// | Level |  Type  | Field |    Description     |
	// +-------+--------+-------+--------------------+
	// |     0 | append |       |  -> append         |
	// |     1 | render |       |    -> render       |
	// |     2 | scan   |       |       -> scan      |
	// |     2 |        | table |          t@primary |
	// |     1 | render |       |    -> render       |
	// |     2 | scan   |       |       -> scan      |
	// |     2 |        | table |          t@primary |
	// +-------+--------+-------+--------------------+
	// (7 rows)
}

func Example_user() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	c.Run("user ls")
	c.Run("user ls --format=pretty")
	c.Run("user ls --format=tsv")
	c.Run("user set foo")
	// Don't use get, since the output of hashedPassword is random.
	// c.Run("user get foo")
	c.Run("user ls --format=pretty")
	c.Run("user rm foo")
	c.Run("user ls --format=pretty")

	// Output:
	// user ls
	// 0 rows
	// username
	// user ls --format=pretty
	// +----------+
	// | username |
	// +----------+
	// +----------+
	// (0 rows)
	// user ls --format=tsv
	// 0 rows
	// username
	// user set foo
	// INSERT 1
	// user ls --format=pretty
	// +----------+
	// | username |
	// +----------+
	// | foo      |
	// +----------+
	// (1 row)
	// user rm foo
	// DELETE 1
	// user ls --format=pretty
	// +----------+
	// | username |
	// +----------+
	// +----------+
	// (0 rows)
}

// TestFlagUsage is a basic test to make sure the fragile
// help template does not break.
func TestFlagUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Override os.Stdout with our own.
	old := os.Stdout
	defer func() {
		os.Stdout = old
	}()

	r, w, _ := os.Pipe()
	os.Stdout = w

	done := make(chan struct{})
	var buf bytes.Buffer
	// copy the output in a separate goroutine so printing can't block indefinitely.
	go func() {
		// Copy reads 'r' until EOF is reached.
		_, _ = io.Copy(&buf, r)
		close(done)
	}()

	if err := Run([]string{"help"}); err != nil {
		fmt.Println(err)
	}

	// back to normal state
	w.Close()
	<-done

	// Filter out all test flags.
	testFlagRE := regexp.MustCompile(`--(test\.|verbosity|vmodule)`)
	lines := strings.Split(buf.String(), "\n")
	final := []string{}
	for _, l := range lines {
		if testFlagRE.MatchString(l) {
			continue
		}
		final = append(final, l)
	}
	got := strings.Join(final, "\n")
	expected := `CockroachDB command-line interface and server.

Usage:
  cockroach [command]

Available Commands:
  start          start a node
  cert           create ca, node, and client certs
  freeze-cluster freeze the cluster in preparation for an update
  quit           drain and shutdown node

  sql            open a sql shell
  user           get, set, list and remove users
  zone           get, set, list and remove zones
  node           list nodes and show their status
  dump           dump sql tables

  gen            generate auxiliary files
  version        output version information
  debug          debugging commands

Flags:
      --alsologtostderr Severity[=INFO]   logs at or above this threshold go to stderr (default INFO)
      --log-backtrace-at traceLocation    when logging hits line file:N, emit a stack trace (default :0)
      --log-dir string                    if non-empty, write log files in this directory
      --logtostderr                       log to standard error instead of files
      --no-color                          disable standard error log colorization

Use "cockroach [command] --help" for more information about a command.
`

	if got != expected {
		t.Errorf("got:\n%s\n----\nexpected:\n%s", got, expected)
	}
}

func Example_node() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	// Refresh time series data, which is required to retrieve stats.
	if err := c.WriteSummaries(); err != nil {
		log.Fatalf(context.Background(), "Couldn't write stats summaries: %s", err)
	}

	c.Run("node ls")
	c.Run("node ls --format=pretty")
	c.Run("node status 10000")

	// Output:
	// node ls
	// 1 row
	// id
	// 1
	// node ls --format=pretty
	// +----+
	// | id |
	// +----+
	// |  1 |
	// +----+
	// (1 row)
	// node status 10000
	// Error: node 10000 doesn't exist
}

func TestFreeze(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	assertOutput := func(msg string) {
		if !strings.HasSuffix(strings.TrimSpace(msg), "ok") {
			t.Fatal(errors.Errorf("expected trailing 'ok':\n%s", msg))
		}
	}

	{
		out, err := c.RunWithCapture("freeze-cluster")
		if err != nil {
			t.Fatal(err)
		}
		assertOutput(out)
	}
	{
		out, err := c.RunWithCapture("freeze-cluster --undo")
		if err != nil {
			t.Fatal(err)
		}
		assertOutput(out)
	}
}

func TestNodeStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#13394")

	start := timeutil.Now()
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	// Refresh time series data, which is required to retrieve stats.
	if err := c.WriteSummaries(); err != nil {
		t.Fatalf("couldn't write stats summaries: %s", err)
	}

	out, err := c.RunWithCapture("node status 1 --format=pretty")
	if err != nil {
		t.Fatal(err)
	}
	checkNodeStatus(t, c, out, start)

	out, err = c.RunWithCapture("node status --format=pretty")
	if err != nil {
		t.Fatal(err)
	}
	checkNodeStatus(t, c, out, start)
}

func checkNodeStatus(t *testing.T, c cliTest, output string, start time.Time) {
	buf := bytes.NewBufferString(output)
	s := bufio.NewScanner(buf)

	// Skip command line.
	if !s.Scan() {
		t.Fatalf("Couldn't skip command line: %s", s.Err())
	}

	checkSeparatorLine(t, s)

	// check column names.
	if !s.Scan() {
		t.Fatalf("Error reading column names: %s", s.Err())
	}
	cols, err := extractFields(s.Text())
	if err != nil {
		t.Fatalf("%s", err)
	}
	if !reflect.DeepEqual(cols, nodesColumnHeaders) {
		t.Fatalf("columns (%s) don't match expected (%s)", cols, nodesColumnHeaders)
	}

	checkSeparatorLine(t, s)

	// Check node status.
	if !s.Scan() {
		t.Fatalf("error reading node status: %s", s.Err())
	}
	fields, err := extractFields(s.Text())
	if err != nil {
		t.Fatalf("%s", err)
	}

	nodeID := c.Gossip().NodeID.Get()
	nodeIDStr := strconv.FormatInt(int64(nodeID), 10)
	if a, e := fields[0], nodeIDStr; a != e {
		t.Errorf("node id (%s) != expected (%s)", a, e)
	}

	nodeAddr, err := c.Gossip().GetNodeIDAddress(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := fields[1], nodeAddr.String(); a != e {
		t.Errorf("node address (%s) != expected (%s)", a, e)
	}

	// Verify Build Tag.
	if a, e := fields[2], build.GetInfo().Tag; a != e {
		t.Errorf("build tag (%s) != expected (%s)", a, e)
	}

	// Verify that updated_at and started_at are reasonably recent.
	// CircleCI can be very slow. This was flaky at 5s.
	checkTimeElapsed(t, fields[3], 15*time.Second, start)
	checkTimeElapsed(t, fields[4], 15*time.Second, start)

	// Verify all byte/range metrics.
	testcases := []struct {
		name   string
		idx    int
		maxval int64
	}{
		{"live_bytes", 5, 40000},
		{"key_bytes", 6, 30000},
		{"value_bytes", 7, 40000},
		{"intent_bytes", 8, 30000},
		{"system_bytes", 9, 30000},
		{"leader_ranges", 10, 3},
		{"repl_ranges", 11, 3},
		{"avail_ranges", 12, 3},
	}
	for _, tc := range testcases {
		val, err := strconv.ParseInt(fields[tc.idx], 10, 64)
		if err != nil {
			t.Errorf("couldn't parse %s '%s': %v", tc.name, fields[tc.idx], err)
			continue
		}
		if val < 0 {
			t.Errorf("value for %s (%d) cannot be less than 0", tc.name, val)
			continue
		}
		if val > tc.maxval {
			t.Errorf("value for %s (%d) greater than max (%d)", tc.name, val, tc.maxval)
		}
	}

	checkSeparatorLine(t, s)
}

var separatorLineExp = regexp.MustCompile(`[\+-]+$`)

func checkSeparatorLine(t *testing.T, s *bufio.Scanner) {
	if !s.Scan() {
		t.Fatalf("error reading separator line: %s", s.Err())
	}
	if !separatorLineExp.MatchString(s.Text()) {
		t.Fatalf("separator line not found: %s", s.Text())
	}
}

// checkRecentTime produces a test error if the time is not within the specified number
// of seconds of the given start time.
func checkTimeElapsed(t *testing.T, timeStr string, elapsed time.Duration, start time.Time) {
	// Truncate start time, because the CLI currently outputs times with a second-level
	// granularity.
	start = start.Truncate(time.Second)
	tm, err := time.ParseInLocation(localTimeFormat, timeStr, start.Location())
	if err != nil {
		t.Errorf("couldn't parse time '%s': %s", timeStr, err)
		return
	}
	end := start.Add(elapsed)
	if tm.Before(start) || tm.After(end) {
		t.Errorf("time (%s) not within range [%s,%s]", tm, start, end)
	}
}

// extractFields extracts the fields from a pretty-printed row of SQL output,
// discarding excess whitespace and column separators.
func extractFields(line string) ([]string, error) {
	fields := strings.Split(line, "|")
	// fields has two extra entries, one for the empty token to the left of the first
	// |, and another empty one to the right of the final |. So, we need to take those
	// out.
	if a, e := len(fields), len(nodesColumnHeaders)+2; a != e {
		return nil, errors.Errorf("can't extract fields: # of fields (%d) != expected (%d)", a, e)
	}
	fields = fields[1 : len(fields)-1]
	var r []string
	for _, f := range fields {
		r = append(r, strings.TrimSpace(f))
	}
	return r, nil
}

func TestGenMan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Generate man pages in a temp directory.
	manpath, err := ioutil.TempDir("", "TestGenMan")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(manpath); err != nil {
			t.Errorf("couldn't remove temporary directory %s: %s", manpath, err)
		}
	}()
	if err := Run([]string{"gen", "man", "--path=" + manpath}); err != nil {
		t.Fatal(err)
	}

	// Ensure we have a sane number of man pages.
	count := 0
	err = filepath.Walk(manpath, func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, ".1") && !info.IsDir() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if min := 20; count < min {
		t.Errorf("number of man pages (%d) < minimum (%d)", count, min)
	}
}

func TestGenAutocomplete(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Get a unique path to which we can write our autocomplete files.
	acdir, err := ioutil.TempDir("", "TestGenAutoComplete")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(acdir); err != nil {
			t.Errorf("couldn't remove temporary directory %s: %s", acdir, err)
		}
	}()

	const minsize = 25000
	acpath := filepath.Join(acdir, "cockroach.bash")

	if err := Run([]string{"gen", "autocomplete", "--out=" + acpath}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(acpath)
	if err != nil {
		t.Fatal(err)
	}
	if size := info.Size(); size < minsize {
		t.Fatalf("autocomplete file size (%d) < minimum (%d)", size, minsize)
	}
}

func TestJunkPositionalArguments(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t, noServer: true})
	defer c.cleanup()

	for i, test := range []struct {
		cmd        *cobra.Command
		invocation string
	}{
		{startCmd, "start junk"},
		{sqlShellCmd, "sql junk"},
		{genManCmd, "gen man junk"},
		{genAutocompleteCmd, "gen autocomplete junk"},
		{genExamplesCmd, "gen example-data file junk"},
	} {
		out, err := c.RunWithCapture(test.invocation)
		if err != nil {
			t.Fatal(errors.Wrap(err, strconv.Itoa(i)))
		}
		exp := test.invocation + "\ninvalid arguments\n"
		if exp != out {
			t.Errorf("expected:\n%s\ngot:\n%s", exp, out)
		}
	}
}
