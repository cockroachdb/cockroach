// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	// Shows sample output for the following packages, some of which runs with
	// non-default configurations.
	pkgs := map[string]struct{}{
		baseImportPath + "kv/kvnemesis":  {},
		baseImportPath + "sql/logictest": {},
		baseImportPath + "storage":       {},
	}

	runTC(func(buildID string, opts map[string]string) {
		pkg := opts["env.PKG"]
		if _, ok := pkgs[pkg]; !ok {
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
	// github.com/cockroachdb/cockroach/pkg/kv/kvnemesis
	//   env.COCKROACH_KVNEMESIS_STEPS: 10000
	//   env.GOFLAGS:     -parallel=4
	//   env.STRESSFLAGS: -maxruns 0 -maxtime 1h0m0s -maxfails 1 -p 4
	//   env.TESTTIMEOUT: 40m0s
	//
	// github.com/cockroachdb/cockroach/pkg/kv/kvnemesis
	//   env.COCKROACH_KVNEMESIS_STEPS: 10000
	//   env.GOFLAGS:     -race -parallel=4
	//   env.STRESSFLAGS: -maxruns 0 -maxtime 1h0m0s -maxfails 1 -p 1
	//   env.TESTTIMEOUT: 40m0s
	//
	// github.com/cockroachdb/cockroach/pkg/sql/logictest
	//   env.GOFLAGS:     -parallel=2
	//   env.STRESSFLAGS: -maxruns 100 -maxtime 3h0m0s -maxfails 1 -p 2
	//   env.TESTTIMEOUT: 2h0m0s
	//
	// github.com/cockroachdb/cockroach/pkg/sql/logictest
	//   env.GOFLAGS:     -race -parallel=2
	//   env.STRESSFLAGS: -maxruns 100 -maxtime 3h0m0s -maxfails 1 -p 1
	//   env.TESTTIMEOUT: 2h0m0s
	//
	// github.com/cockroachdb/cockroach/pkg/storage
	//   env.GOFLAGS:     -parallel=4
	//   env.STRESSFLAGS: -maxruns 100 -maxtime 1h0m0s -maxfails 1 -p 4
	//   env.TESTTIMEOUT: 40m0s
	//
	// github.com/cockroachdb/cockroach/pkg/storage
	//   env.GOFLAGS:     -race -parallel=4
	//   env.STRESSFLAGS: -maxruns 100 -maxtime 1h0m0s -maxfails 1 -p 1
	//   env.TESTTIMEOUT: 40m0s
}
