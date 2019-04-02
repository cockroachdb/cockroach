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

package main

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

func TestRunTC(t *testing.T) {
	count := 0
	runTC(func(buildID string, opts map[string]string) {
		count++
		if pkg, ok := opts["env.PKG"]; ok {
			if strings.Contains(pkg, "/vendor/") {
				t.Errorf("unexpected package %s", pkg)
			}
		} else {
			t.Errorf("parameters did not include package: %+v", opts)
		}
	})
	if count == 0 {
		t.Fatal("no builds were created")
	}
}

func Example_runTC() {
	// Shows sample output for two packages, one of which runs with reduced
	// parallelism.
	runTC(func(buildID string, opts map[string]string) {
		pkg := opts["env.PKG"]
		if !strings.HasSuffix(pkg, "pkg/sql/logictest") && !strings.HasSuffix(pkg, "pkg/storage") {
			return
		}
		var keys []string
		for k := range opts {
			if k != "env.PKG" {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		fmt.Println(pkg)
		for _, k := range keys {
			fmt.Printf("  %-16s %s\n", k+":", opts[k])
		}
		fmt.Println()
	})

	// Output:
	// github.com/cockroachdb/cockroach/pkg/sql/logictest
	//   env.GOFLAGS:     -parallel=2
	//   env.STRESSFLAGS: -p 2
	//
	// github.com/cockroachdb/cockroach/pkg/sql/logictest
	//   env.GOFLAGS:     -race -parallel=1
	//   env.STRESSFLAGS: -p 1
	//
	// github.com/cockroachdb/cockroach/pkg/storage
	//   env.GOFLAGS:     -parallel=4
	//   env.STRESSFLAGS: -p 4
	//
	// github.com/cockroachdb/cockroach/pkg/storage
	//   env.GOFLAGS:     -race -parallel=2
	//   env.STRESSFLAGS: -p 2
}
