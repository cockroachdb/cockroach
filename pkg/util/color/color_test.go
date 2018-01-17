// Copyright 2017 The Cockroach Authors.
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

package color

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestColor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func(prev Profile) { StdoutProfile = prev }(StdoutProfile)
	defer func(prev *os.File) { os.Stdout = prev }(os.Stdout)

	for _, tc := range []struct {
		name       string
		profile    Profile
		red, reset string
	}{
		{"nil", nil, "", ""},
		{"profile8", profile8, "\033[0;31;49m", "\033[0m"},
		{"profile256", profile256, "\033[38;5;160m", "\033[0m"},
	} {
		t.Run(fmt.Sprintf("profile=%s", tc.name), func(t *testing.T) {
			StdoutProfile = tc.profile
			tmpFile, err := ioutil.TempFile("", tc.name)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.Remove(tmpFile.Name()) }()
			defer tmpFile.Close()
			os.Stdout = tmpFile

			fmt.Print("Normal")
			Stdout(Red)
			fmt.Print("Red")
			Stdout(Reset)
			fmt.Print("Normal")
			fmt.Printf("%s", StdoutProfile[Red])
			fmt.Print("Red")
			fmt.Printf("%s", StdoutProfile[Reset])

			out, err := ioutil.ReadFile(tmpFile.Name())
			if err != nil {
				t.Fatal(err)
			}
			e := fmt.Sprintf("Normal%[1]sRed%[2]sNormal%[1]sRed%[2]s", tc.red, tc.reset)
			if a := string(out); e != a {
				t.Errorf("expected %q, but got %q", e, a)
			}
		})
	}
}

// Suppress unused error about Stderr.
var _ = Stderr
