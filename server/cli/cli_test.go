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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter.mattis@gmail.com)

package cli

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
)

type cliTest struct {
	*server.TestServer
}

func newCLITest() cliTest {
	// Reset the client context for each test. We don't reset the
	// pointer (because they are tied into the flags), but instead
	// overwrite the existing struct's values.
	*Context = *server.NewContext()

	osExit = func(int) {}
	osStderr = os.Stdout

	s := &server.TestServer{}
	if err := s.Start(); err != nil {
		log.Fatalf("Could not start server: %v", err)
	}

	return cliTest{TestServer: s}
}

func (c cliTest) Run(line string) {
	a := strings.Fields(line)

	var args []string
	args = append(args, a[0])
	args = append(args, fmt.Sprintf("--addr=%s", c.ServingAddr()))
	// Always load test certs.
	args = append(args, fmt.Sprintf("--certs=%s", security.EmbeddedCertsDir))
	args = append(args, a[1:]...)

	fmt.Fprintf(os.Stderr, "%s\n", args)
	fmt.Printf("%s\n", line)
	if err := Run(args); err != nil {
		fmt.Printf("%s\n", err)
	}
}

func ExampleBasic() {
	c := newCLITest()

	c.Run("kv put a 1 b 2")
	c.Run("kv scan")
	c.Run("kv del a")
	c.Run("kv get a")
	c.Run("kv get b")
	c.Run("kv inc c 1")
	c.Run("kv inc c 10")
	c.Run("kv inc c 100")
	c.Run("kv scan")
	c.Run("kv inc c b")
	c.Run("quit")

	// Output:
	// kv put a 1 b 2
	// kv scan
	// "a"	"1"
	// "b"	"2"
	// kv del a
	// kv get a
	// "a" not found
	// kv get b
	// 2
	// kv inc c 1
	// 1
	// kv inc c 10
	// 11
	// kv inc c 100
	// 111
	// kv scan
	// "b"	"2"
	// "c"	111
	// kv inc c b
	// invalid increment: b: strconv.ParseInt: parsing "b": invalid syntax
	// quit
	// node drained and shutdown: ok
}

func ExampleQuoted() {
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
	// kv get a\x00
	// 日本語
	// kv del a\x00
	// kv inc 1\x01
	// 1
	// kv get 1\x01
	// 1
	// quit
	// node drained and shutdown: ok
}

func ExampleInsecure() {
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
	// quit --insecure
	// node drained and shutdown: ok
}

func ExampleSplitMergeRanges() {
	c := newCLITest()

	c.Run("kv put a 1 b 2 c 3 d 4")
	c.Run("kv scan")
	c.Run("range split c c")
	c.Run("range ls")
	c.Run("kv scan")
	c.Run("range merge b")
	c.Run("range ls")
	c.Run("kv scan")
	c.Run("quit")

	// Output:
	// kv put a 1 b 2 c 3 d 4
	// kv scan
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// "d"	"4"
	// range split c c
	// range ls
	// ""-"c" [1]
	// 	0: node-id=1 store-id=1 attrs=[]
	// "c"-"\xff\xff" [2]
	// 	0: node-id=1 store-id=1 attrs=[]
	// kv scan
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// "d"	"4"
	// range merge b
	// range ls
	// ""-"\xff\xff" [1]
	// 	0: node-id=1 store-id=1 attrs=[]
	// kv scan
	// "a"	"1"
	// "b"	"2"
	// "c"	"3"
	// "d"	"4"
	// quit
	// node drained and shutdown: ok
}

func ExampleGlogFlags() {
	c := newCLITest()

	c.Run("kv --alsologtostderr=false scan")
	c.Run("kv --log-backtrace-at=foo.go:1 scan")
	c.Run("kv --log-dir='' scan")
	c.Run("kv --logtostderr scan")
	c.Run("kv --stderrthreshold=2 scan")
	c.Run("kv --v=0 scan")
	c.Run("kv --vmodule=foo=1 scan")

	// Output:
	// kv --alsologtostderr=false scan
	// kv --log-backtrace-at=foo.go:1 scan
	// kv --log-dir='' scan
	// kv --logtostderr scan
	// kv --stderrthreshold=2 scan
	// kv --v=0 scan
	// kv --vmodule=foo=1 scan
}
