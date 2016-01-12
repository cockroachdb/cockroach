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
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

type cliTest struct {
	*server.TestServer
}

func newCLITest() cliTest {
	// Reset the client context for each test. We don't reset the
	// pointer (because they are tied into the flags), but instead
	// overwrite the existing struct's values.
	context.InitDefaults()

	osStderr = os.Stdout

	s := &server.TestServer{}
	if err := s.Start(); err != nil {
		log.Fatalf("Could not start server: %v", err)
	}

	return cliTest{TestServer: s}
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
			err = util.Errorf("panic: %v", x)
		}
	}()

	// Run the command. The output will be returned in the defer block.
	c.Run(line)
	return
}

func (c cliTest) RunWithArgs(a []string) {
	var args []string
	args = append(args, a[0])
	h, err := c.ServingHost()
	if err != nil {
		fmt.Println(err)
	}
	p, err := c.ServingPort()
	if err != nil {
		fmt.Println(err)
	}
	pg, err := c.PGPort()
	if err != nil {
		fmt.Println(err)
	}
	args = append(args, fmt.Sprintf("--host=%s", h))
	if a[0] == "kv" || a[0] == "quit" || a[0] == "range" || a[0] == "exterminate" {
		args = append(args, fmt.Sprintf("--cockroach-port=%s", p))
	} else {
		args = append(args, fmt.Sprintf("--port=%s", pg))
	}
	// Always load test certs.
	args = append(args, fmt.Sprintf("--certs=%s", security.EmbeddedCertsDir))
	args = append(args, a[1:]...)

	fmt.Fprintf(os.Stderr, "%s\n", args)
	fmt.Println(strings.Join(a, " "))
	if err := Run(args); err != nil {
		fmt.Println(err)
	}
}

func TestQuit(t *testing.T) {
	defer leaktest.AfterTest(t)
	c := newCLITest()
	c.Run("quit")
	// Wait until this async command stops the server.
	<-c.Stopper().IsStopped()
}

func Example_basic() {
	c := newCLITest()
	defer c.Stop()

	c.Run("kv put a 1 b 2 c 3")
	c.Run("kv scan")
	c.Run("kv revscan")
	c.Run("kv del a c")
	c.Run("kv get a")
	c.Run("kv get b")
	c.Run("kv inc c 1")
	c.Run("kv inc c 10")
	c.Run("kv inc c 100")
	c.Run("kv inc c -- -60")
	c.Run("kv inc c -- -9")
	c.Run("kv scan")
	c.Run("kv revscan")
	c.Run("kv inc c b")

	// Output:
	// kv put a 1 b 2 c 3
	// kv scan
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// 3 result(s)
	// kv revscan
	// "c"	"3"
	// "b"	"2"
	// "a"	"1"
	// 3 result(s)
	// kv del a c
	// kv get a
	// "a" not found
	// kv get b
	// "2"
	// kv inc c 1
	// 1
	// kv inc c 10
	// 11
	// kv inc c 100
	// 111
	// kv inc c -- -60
	// 51
	// kv inc c -- -9
	// 42
	// kv scan
	// "b"	"2"
	// "c"	42
	// 2 result(s)
	// kv revscan
	// "c"	42
	// "b"	"2"
	// 2 result(s)
	// kv inc c b
	// invalid increment: b: strconv.ParseInt: parsing "b": invalid syntax
}

func Example_quoted() {
	c := newCLITest()
	defer c.Stop()

	c.Run(`kv put a\x00 日本語`)                                  // UTF-8 input text
	c.Run(`kv put a\x01 \u65e5\u672c\u8a9e`)                   // explicit Unicode code points
	c.Run(`kv put a\x02 \U000065e5\U0000672c\U00008a9e`)       // explicit Unicode code points
	c.Run(`kv put a\x03 \xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e`) // explicit UTF-8 bytes
	c.Run(`kv scan`)
	c.Run(`kv get a\x00`)
	c.Run(`kv del a\x00`)
	c.Run(`kv inc 1\x01`)
	c.Run(`kv get 1\x01`)

	// Output:
	// kv put a\x00 日本語
	// kv put a\x01 \u65e5\u672c\u8a9e
	// kv put a\x02 \U000065e5\U0000672c\U00008a9e
	// kv put a\x03 \xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e
	// kv scan
	// "a\x00"	"日本語"
	// "a\x01"	"日本語"
	// "a\x02"	"日本語"
	// "a\x03"	"日本語"
	// 4 result(s)
	// kv get a\x00
	// "日本語"
	// kv del a\x00
	// kv inc 1\x01
	// 1
	// kv get 1\x01
	// 1
}

func Example_insecure() {
	c := cliTest{}
	c.TestServer = &server.TestServer{}
	c.Ctx = server.NewTestContext()
	c.Ctx.Insecure = true
	if err := c.Start(); err != nil {
		log.Fatalf("Could not start server: %v", err)
	}
	defer c.Stop()

	c.Run("kv --insecure put a 1 b 2")
	c.Run("kv --insecure scan")

	// Output:
	// kv --insecure put a 1 b 2
	// kv --insecure scan
	// "a"	"1"
	// "b"	"2"
	// 2 result(s)
}

func Example_ranges() {
	c := newCLITest()
	defer c.Stop()

	c.Run("kv put a 1 b 2 c 3 d 4")
	c.Run("kv scan")
	c.Run("kv revscan")
	c.Run("range split c")
	c.Run("range ls")
	c.Run("kv scan")
	c.Run("kv revscan")
	c.Run("range merge b")
	c.Run("range ls")
	c.Run("kv scan")
	c.Run("kv revscan")
	c.Run("kv delrange a c")
	c.Run("kv scan")

	// Output:
	// kv put a 1 b 2 c 3 d 4
	// kv scan
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// "d"	"4"
	// 4 result(s)
	// kv revscan
	// "d"	"4"
	// "c"	"3"
	// "b"	"2"
	// "a"	"1"
	// 4 result(s)
	// range split c
	// range ls
	// /Min-"c" [1]
	// 	0: node-id=1 store-id=1
	// "c"-/Table/11 [4]
	// 	0: node-id=1 store-id=1
	// /Table/11-/Table/12 [2]
	// 	0: node-id=1 store-id=1
	// /Table/12-/Max [3]
	// 	0: node-id=1 store-id=1
	// 4 result(s)
	// kv scan
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// "d"	"4"
	// 4 result(s)
	// kv revscan
	// "d"	"4"
	// "c"	"3"
	// "b"	"2"
	// "a"	"1"
	// 4 result(s)
	// range merge b
	// range ls
	// /Min-/Table/11 [1]
	// 	0: node-id=1 store-id=1
	// /Table/11-/Table/12 [2]
	// 	0: node-id=1 store-id=1
	// /Table/12-/Max [3]
	// 	0: node-id=1 store-id=1
	// 3 result(s)
	// kv scan
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// "d"	"4"
	// 4 result(s)
	// kv revscan
	// "d"	"4"
	// "c"	"3"
	// "b"	"2"
	// "a"	"1"
	// 4 result(s)
	// kv delrange a c
	// kv scan
	// "c"	"3"
	// "d"	"4"
	// 2 result(s)
}

func Example_logging() {
	c := newCLITest()
	defer c.Stop()

	c.Run("kv --alsologtostderr=false scan")
	c.Run("kv --log-backtrace-at=foo.go:1 scan")
	c.Run("kv --log-dir='' scan")
	c.Run("kv --logtostderr=true scan")
	c.Run("kv --verbosity=0 scan")
	c.Run("kv --vmodule=foo=1 scan")

	// Output:
	// kv --alsologtostderr=false scan
	// 0 result(s)
	// kv --log-backtrace-at=foo.go:1 scan
	// 0 result(s)
	// kv --log-dir='' scan
	// 0 result(s)
	// kv --logtostderr=true scan
	// 0 result(s)
	// kv --verbosity=0 scan
	// 0 result(s)
	// kv --vmodule=foo=1 scan
	// 0 result(s)
}

func Example_cput() {
	c := newCLITest()
	defer c.Stop()

	c.Run("kv put a 1 b 2 c 3 d 4")
	c.Run("kv scan")
	c.Run("kv cput e 5")
	c.Run("kv cput b 3 2")
	c.Run("kv scan")

	// Output:
	// kv put a 1 b 2 c 3 d 4
	// kv scan
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// "d"	"4"
	// 4 result(s)
	// kv cput e 5
	// kv cput b 3 2
	// kv scan
	// "a"	"1"
	// "b"	"3"
	// "c"	"3"
	// "d"	"4"
	// "e"	"5"
	// 5 result(s)
}

func Example_max_results() {
	c := newCLITest()
	defer c.Stop()

	c.Run("kv put a 1 b 2 c 3 d 4")
	c.Run("kv scan --max-results=3")
	c.Run("kv revscan --max-results=2")
	c.Run("range split c")
	c.Run("range split d")
	c.Run("range ls --max-results=2")

	// Output:
	// kv put a 1 b 2 c 3 d 4
	// kv scan --max-results=3
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// 3 result(s)
	// kv revscan --max-results=2
	// "d"	"4"
	// "c"	"3"
	// 2 result(s)
	// range split c
	// range split d
	// range ls --max-results=2
	// /Min-"c" [1]
	// 	0: node-id=1 store-id=1
	// "c"-"d" [4]
	// 	0: node-id=1 store-id=1
	// 2 result(s)
}

func Example_zone() {
	/*
			c := newCLITest()

			zone100 := `replicas:
		- attrs: [us-east-1a,ssd]
		- attrs: [us-east-1b,ssd]
		- attrs: [us-west-1b,ssd]
		range_min_bytes: 8388608
		range_max_bytes: 67108864
		`
			c.Run("zone ls")
			// Call RunWithArgs to bypass the "split-by-whitespace" arg builder.
			c.RunWithArgs([]string{"zone", "set", "100", zone100})
			c.Run("zone ls")
			c.Run("zone get 100")
			c.Run("zone rm 100")
			c.Run("zone ls")
			c.Run("quit")

			// Output:
			// zone ls
			// zone set 100 replicas:
			// - attrs: [us-east-1a,ssd]
			// - attrs: [us-east-1b,ssd]
			// - attrs: [us-west-1b,ssd]
			// range_min_bytes: 8388608
			// range_max_bytes: 67108864
			//
			// OK
			// zone ls
			// Object 100:
			// replicas:
			// - attrs: [us-east-1a, ssd]
			// - attrs: [us-east-1b, ssd]
			// - attrs: [us-west-1b, ssd]
			// range_min_bytes: 8388608
			// range_max_bytes: 67108864
			//
			// zone get 100
			// replicas:
			// - attrs: [us-east-1a, ssd]
			// - attrs: [us-east-1b, ssd]
			// - attrs: [us-west-1b, ssd]
			// range_min_bytes: 8388608
			// range_max_bytes: 67108864
			//
			// zone rm 100
			// OK
			// zone ls
			// quit
			// node drained and shutdown: ok
	*/
}

func Example_user() {
	/*
		c := newCLITest()

		c.Run("user ls")
		c.Run("user set foo --password=bar")
		// Don't use get, since the output of hashedPassword is random.
		// c.Run("user get foo")
		c.Run("user ls")
		c.Run("user rm foo")
		c.Run("user ls")
		c.Run("quit")

		// Output:
		// user ls
		// +----------+
		// | username |
		// +----------+
		// +----------+
		// user set foo --password=bar
		// OK
		// user ls
		// +----------+
		// | username |
		// +----------+
		// | foo      |
		// +----------+
		// user rm foo
		// OK
		// user ls
		// +----------+
		// | username |
		// +----------+
		// +----------+
		// quit
		// node drained and shutdown: ok
	*/
}

// TestFlagUsage is a basic test to make sure the fragile
// help template does not break.
func TestFlagUsage(t *testing.T) {
	defer leaktest.AfterTest(t)

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
	testFlagRE := regexp.MustCompile(`--test\.`)
	lines := strings.Split(buf.String(), "\n")
	final := []string{}
	for _, l := range lines {
		if testFlagRE.MatchString(l) {
			continue
		}
		final = append(final, l)
	}
	got := strings.Join(final, "\n")
	expected := `Usage:
  cockroach [command]

Available Commands:
  init        init new Cockroach cluster
  start       start a node by joining the gossip network
  cert        create ca, node, and client certs
  exterminate destroy all data held by the node
  quit        drain and shutdown node

  log         make log files human-readable

  sql         open a sql shell
  kv          get, put, conditional put, increment, delete, scan, and reverse scan key/value pairs
  user        get, set, list and remove users
  range       list, split and merge ranges
  zone        get, set, list and remove zones
  node        list nodes and show their status

  version     output version information

Flags:
      --alsologtostderr         log to standard error as well as files
      --color                   colorize standard error output according to severity (default "auto")
      --log-backtrace-at        when logging hits line file:N, emit a stack trace (default :0)
      --log-dir                 if non-empty, write log files in this directory
      --logtostderr             log to standard error instead of files (default true)
      --verbosity               log level for V logs
      --vmodule                 comma-separated list of pattern=N settings for file-filtered logging

Use "cockroach [command] --help" for more information about a command.
`

	if got != expected {
		t.Errorf("got:\n%s\n----\nexpected:\n%s", got, expected)
	}
}

func Example_Node() {
	c := newCLITest()
	defer c.Stop()

	// Refresh time series data, which is required to retrieve stats.
	if err := c.TestServer.WriteSummaries(); err != nil {
		log.Fatalf("Couldn't write stats summaries: %s", err)
	}

	c.Run("node ls")
	c.Run("node status 10000")

	// Output:
	// node ls
	// +----+
	// | id |
	// +----+
	// |  1 |
	// +----+
	// node status 10000
	// Error: node 10000 doesn't exist
}

func TestNodeStatus(t *testing.T) {
	defer leaktest.AfterTest(t)

	start := time.Now()
	c := newCLITest()
	defer c.Stop()

	// Refresh time series data, which is required to retrieve stats.
	if err := c.TestServer.WriteSummaries(); err != nil {
		t.Fatalf("couldn't write stats summaries: %s", err)
	}

	out, err := c.RunWithCapture("node status 1")
	if err != nil {
		t.Fatal(err)
	}
	checkNodeStatus(t, c, out, start)

	out, err = c.RunWithCapture("node status")
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

	nodeID := c.Gossip().GetNodeID()
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

	// Verify that updated_at and started_at are reasonably recent.
	checkTimeElapsed(t, fields[2], 5*time.Second, start)
	checkTimeElapsed(t, fields[3], 5*time.Second, start)

	// Verify all byte/range metrics.
	testcases := []struct {
		name   string
		idx    int
		maxval int64
	}{
		{"live_bytes", 4, 10000},
		{"key_bytes", 5, 10000},
		{"value_bytes", 6, 10000},
		{"intent_bytes", 7, 10000},
		{"system_bytes", 8, 10000},
		{"leader_ranges", 9, 3},
		{"repl_ranges", 10, 3},
		{"avail_ranges", 11, 3},
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
		return nil, util.Errorf("can't extract fields: # of fields (%d) != expected (%d)", a, e)
	}
	fields = fields[1 : len(fields)-1]
	var r []string
	for _, f := range fields {
		r = append(r, strings.TrimSpace(f))
	}
	return r, nil
}
