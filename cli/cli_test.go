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
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
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

func Example_basic() {
	c := newCLITest()

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
	c.Run("quit")

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
	// quit
	// node drained and shutdown: ok
}

func Example_quoted() {
	c := newCLITest()

	c.Run(`kv put a\x00 日本語`)                                  // UTF-8 input text
	c.Run(`kv put a\x01 \u65e5\u672c\u8a9e`)                   // explicit Unicode code points
	c.Run(`kv put a\x02 \U000065e5\U0000672c\U00008a9e`)       // explicit Unicode code points
	c.Run(`kv put a\x03 \xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e`) // explicit UTF-8 bytes
	c.Run(`kv scan`)
	c.Run(`kv get a\x00`)
	c.Run(`kv del a\x00`)
	c.Run(`kv inc 1\x01`)
	c.Run(`kv get 1\x01`)
	c.Run("quit")

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
	// quit
	// node drained and shutdown: ok
}

func Example_insecure() {
	c := cliTest{}
	c.TestServer = &server.TestServer{}
	c.Ctx = server.NewTestContext()
	c.Ctx.Insecure = true
	if err := c.Start(); err != nil {
		log.Fatalf("Could not start server: %v", err)
	}

	c.Run("kv --insecure put a 1 b 2")
	c.Run("kv --insecure scan")
	c.Run("quit --insecure")

	// Output:
	// kv --insecure put a 1 b 2
	// kv --insecure scan
	// "a"	"1"
	// "b"	"2"
	// 2 result(s)
	// quit --insecure
	// node drained and shutdown: ok
}

func Example_ranges() {
	c := newCLITest()

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
	c.Run("quit")

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
	// quit
	// node drained and shutdown: ok
}

func Example_logging() {
	c := newCLITest()

	c.Run("kv --alsologtostderr=false scan")
	c.Run("kv --log-backtrace-at=foo.go:1 scan")
	c.Run("kv --log-dir='' scan")
	c.Run("kv --logtostderr=true scan")
	c.Run("kv --verbosity=0 scan")
	c.Run("kv --vmodule=foo=1 scan")
	c.Run("quit")

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
	// quit
	// node drained and shutdown: ok
}

func Example_cput() {
	c := newCLITest()

	c.Run("kv put a 1 b 2 c 3 d 4")
	c.Run("kv scan")
	c.Run("kv cput e 5")
	c.Run("kv cput b 3 2")
	c.Run("kv scan")
	c.Run("quit")

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
	// quit
	// node drained and shutdown: ok
}

func Example_max_results() {
	c := newCLITest()

	c.Run("kv put a 1 b 2 c 3 d 4")
	c.Run("kv scan --max-results=3")
	c.Run("kv revscan --max-results=2")
	c.Run("range split c")
	c.Run("range split d")
	c.Run("range ls --max-results=2")
	c.Run("quit")

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
	// quit
	// node drained and shutdown: ok
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
