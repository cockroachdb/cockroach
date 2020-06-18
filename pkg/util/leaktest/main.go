// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package leaktest

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// COCKROACH_SKIP can contain the name of a file which contains a JSON map
// of tests to skip. The schema of the JSON map looks like this:
//
// {
//    "SkippedTests": {"test1": "#issue",
//                     "test2/subtest": "#issue"}
// }

var skipList map[string]string

func init() {
	if f, ok := os.LookupEnv("COCKROACH_SKIP"); ok {
		if dat, err := ioutil.ReadFile(f); err == nil {
			m := struct {
				SkippedTests map[string]string
			}{}
			if err := json.Unmarshal(dat, &m); err == nil {
				skipList = m.SkippedTests
			}
		}
	}
}
