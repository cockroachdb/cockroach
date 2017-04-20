// Copyright 2016 The Cockroach Authors.
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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestInitInsecure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := startCmd.Flags()

	testCases := []struct {
		args     []string
		insecure bool
		expected string
	}{
		{[]string{}, false, ""},
		{[]string{"--insecure"}, true, ""},
		{[]string{"--insecure=true"}, true, ""},
		{[]string{"--insecure=false"}, false, ""},
		{[]string{"--host", "localhost"}, false, ""},
		{[]string{"--host", "127.0.0.1"}, false, ""},
		{[]string{"--host", "::1"}, false, ""},
		{[]string{"--host", "192.168.1.1"}, false,
			`specify --insecure to listen on external address 192\.168\.1\.1`},
		{[]string{"--insecure", "--host", "192.168.1.1"}, true, ""},
		{[]string{"--host", "localhost", "--advertise-host", "192.168.1.1"}, false, ""},
		{[]string{"--host", "127.0.0.1", "--advertise-host", "192.168.1.1"}, false, ""},
		{[]string{"--host", "::1", "--advertise-host", "192.168.1.1"}, false, ""},
		{[]string{"--insecure", "--host", "192.168.1.1", "--advertise-host", "192.168.1.1"}, true, ""},
		{[]string{"--insecure", "--host", "192.168.1.1", "--advertise-host", "192.168.2.2"}, true, ""},
		// Clear out the flags when done to avoid affecting other tests that rely on the flag state.
		{[]string{"--host", "", "--advertise-host", ""}, false, ""},
	}
	for i, c := range testCases {
		// Reset the context and for every test case.
		serverInsecure = false

		if err := f.Parse(c.args); err != nil {
			t.Fatal(err)
		}
		if c.insecure != serverInsecure {
			t.Fatalf("%d: expected %v, but found %v", i, c.insecure, serverInsecure)
		}
	}
}

func TestStartArgChecking(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := startCmd.Flags()

	testCases := []struct {
		args     []string
		expected string
	}{
		{[]string{}, ``},
		{[]string{`--insecure=blu`}, `parsing "blu": invalid syntax`},
		{[]string{`--store=path=`}, `no value specified`},
		{[]string{`--store=path=blah,path=blih`}, `field was used twice`},
		{[]string{`--store=path=~/blah`}, `store path cannot start with '~'`},
		{[]string{`--store=path=./~/blah`}, ``},
		{[]string{`--store=size=blih`}, `could not parse store size`},
		{[]string{`--store=size=0.005`}, `store size \(0.005\) must be between 1% and 100%`},
		{[]string{`--store=size=100.5`}, `store size \(100.5\) must be between 1% and 100%`},
		{[]string{`--store=size=0.5%`}, `store size \(0.5%\) must be between 1% and 100%`},
		{[]string{`--store=size=0.5%`}, `store size \(0.5%\) must be between 1% and 100%`},
		{[]string{`--store=size=500.0%`}, `store size \(500.0%\) must be between 1% and 100%`},
		{[]string{`--store=size=.5,path=.`}, ``},
		{[]string{`--store=size=50.%,path=.`}, ``},
		{[]string{`--store=size=50%,path=.`}, ``},
		{[]string{`--store=size=50.5%,path=.`}, ``},
		{[]string{`--store=size=-1231MB`}, `store size \(-1231MB\) must be larger than`},
		{[]string{`--store=size=1231B`}, `store size \(1231B\) must be larger than`},
		{[]string{`--store=size=1231BLA`}, `unhandled size name: bla`},
		{[]string{`--store=attrs=bli:bli`}, `duplicate attribute`},
		{[]string{`--store=type=bli`}, `bli is not a valid store type`},
		{[]string{`--store=bla=bli`}, `bla is not a valid store field`},
		{[]string{`--store=size=123gb`}, `no path specified`},
		{[]string{`--store=type=mem`}, `size must be specified for an in memory store`},
		{[]string{`--store=type=mem,path=blah`}, `path specified for in memory store`},
		{[]string{"--store=type=mem,size=1GiB"}, ``},
	}
	for i, c := range testCases {
		// Reset the context and insecure flag for every test case.
		err := f.Parse(c.args)
		if !testutils.IsError(err, c.expected) {
			t.Errorf("%d: expected %q, but found %v", i, c.expected, err)
		}
	}
}

func TestGCProfiles(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, err := ioutil.TempDir("", "TestGCProfile.")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	data := []byte("hello world")
	const prefix = "testprof."

	var expected []string
	var sum int
	for i := 1; i < len(data); i++ {
		p := filepath.Join(dir, fmt.Sprintf("%s%04d", prefix, i))
		err := ioutil.WriteFile(p, data[:i], 0644)
		if err != nil {
			t.Fatal(err)
		}
		expected = append(expected, p)
		sum += len(data[:i])

		otherPath := filepath.Join(dir, fmt.Sprintf("other.%04d", i))
		if err := ioutil.WriteFile(otherPath, data[:i], 0644); err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i < len(data); i++ {
		gcProfiles(dir, prefix, int64(sum))
		paths, err := filepath.Glob(filepath.Join(dir, prefix+"*"))
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(paths)
		if e := expected[i-1:]; !reflect.DeepEqual(e, paths) {
			t.Fatalf("%d: expected\n%s\nfound\n%s\n",
				i, strings.Join(e, "\n"), strings.Join(paths, "\n"))
		}
		sum -= len(data[:i])
	}
}
