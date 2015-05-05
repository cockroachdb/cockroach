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
	// "a"	1
	// "b"	2
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
	// "b"	2
	// "c"	111
	// kv inc c b
	// invalid increment: b: strconv.ParseInt: parsing "b": invalid syntax
	// quit
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
	// "a"	1
	// "b"	2
	// "c"	3
	// "d"	4
	// range split c c
	// range ls
	// ""-"c" [1]
	// 	0: node-id=1 store-id=1 attrs=[]
	// "c"-"\xff\xff" [2]
	// 	0: node-id=1 store-id=1 attrs=[]
	// kv scan
	// "a"	1
	// "b"	2
	// "c"	3
	// "d"	4
	// range merge b
	// range ls
	// ""-"\xff\xff" [1]
	// 	0: node-id=1 store-id=1 attrs=[]
	// kv scan
	// "a"	1
	// "b"	2
	// "c"	3
	// "d"	4
	// quit
	// node drained and shutdown: ok
}
