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
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestInitInsecure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := startCmd.Flags()
	ctx := cliContext

	testCases := []struct {
		args     []string
		insecure bool
		expected string
	}{
		{[]string{}, true, ""},
		{[]string{"--insecure"}, true, ""},
		{[]string{"--insecure=true"}, true, ""},
		{[]string{"--insecure=false"}, false, ""},
		{[]string{"--host", "localhost"}, true, ""},
		{[]string{"--host", "127.0.0.1"}, true, ""},
		{[]string{"--host", "::1"}, true, ""},
		{[]string{"--host", "192.168.1.1"}, true,
			`specify --insecure to listen on external address 192\.168\.1\.1`},
		{[]string{"--insecure", "--host", "192.168.1.1"}, true, ""},
	}
	for i, c := range testCases {
		// Reset the context and insecure flag for every test case.
		ctx.InitDefaults()
		ctx.Insecure = true
		insecure.isSet = false

		if err := f.Parse(c.args); err != nil {
			t.Fatal(err)
		}
		if c.insecure != ctx.Insecure {
			t.Fatalf("%d: expected %v, but found %v", i, c.insecure, ctx.Insecure)
		}

		err := initInsecure()
		if c.expected == "" {
			if err != nil {
				t.Fatalf("%d: expected success, but found %v", i, err)
			}
		} else if !testutils.IsError(err, c.expected) {
			t.Fatalf("%d: expected %s, but found %v", i, c.expected, err)
		}
	}
}
