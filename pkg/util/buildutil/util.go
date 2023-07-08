// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package buildutil

import (
	"os"
	"strings"
)

// RunningUnderTest returns true if we are executing a test binary.
// Use this function instead of direct CrdbTestBuild constant
// to work with tests that do not set appropriate tags explicitly
// (for example when executing test directly using "go test" or when running
// tests from an IDE)
func RunningUnderTest() bool {
	return CrdbTestBuild || likelyRunningUnderTest
}

var likelyRunningUnderTest bool

func init() {
	likelyRunningUnderTest = len(os.Args) > 0 && strings.HasSuffix(os.Args[0], "test")
}
