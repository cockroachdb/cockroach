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
	args = append(args, fmt.Sprintf("-http=%s", c.HTTPAddr))
	args = append(args, fmt.Sprintf("-rpc=%s", c.RPCAddr))
	args = append(args, a[1:]...)

	fmt.Printf("%s\n", line)
	if err := Run(args); err != nil {
		fmt.Printf("%s\n", err)
	}
}

func ExampleBasic() {
	c := newCLITest()
	defer c.Stop()

	c.Run("put a 1 b 2")
	c.Run("scan")
	c.Run("del a")
	c.Run("get a")
	c.Run("get b")
	c.Run("inc c 1")
	c.Run("inc c 10")
	c.Run("inc c 100")
	c.Run("scan")
	c.Run("inc c b")

	// Output:
	// put a 1 b 2
	// scan
	// "a"	1
	// "b"	2
	// del a
	// get a
	// "a" not found
	// get b
	// 2
	// inc c 1
	// 1
	// inc c 10
	// 11
	// inc c 100
	// 111
	// scan
	// "b"	2
	// "c"	111
	// inc c b
	// invalid increment: b: strconv.ParseInt: parsing "b": invalid syntax
}

func ExampleSplitMergeRanges() {
	c := newCLITest()
	defer c.Stop()

	c.Run("put a 1 b 2 c 3 d 4")
	c.Run("scan")
	c.Run("split-range c c")
	c.Run("ls-ranges")
	c.Run("scan")
	c.Run("merge-range b")
	c.Run("ls-ranges")
	c.Run("scan")

	// Output:
	// put a 1 b 2 c 3 d 4
	// scan
	// "a"	1
	// "b"	2
	// "c"	3
	// "d"	4
	// split-range c c
	// ls-ranges
	// ""-"c" [1]
	// 	0: node-id=1 store-id=1 attrs=[]
	// "c"-"\xff\xff" [2]
	// 	0: node-id=1 store-id=1 attrs=[]
	// scan
	// "a"	1
	// "b"	2
	// "c"	3
	// "d"	4
	// merge-range b
	// ls-ranges
	// ""-"\xff\xff" [1]
	// 	0: node-id=1 store-id=1 attrs=[]
	// scan
	// "a"	1
	// "b"	2
	// "c"	3
	// "d"	4
}
