// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logictest

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var testfile string

func init() {
	if bazel.BuiltWithBazel() {
		var err error
		testfile, err = bazel.Runfile("pkg/sql/logictest/testdata/testdata")
		if err != nil {
			panic(err)
		}
	} else {
		testfile = "../../sql/logictest/testdata/testdata"
	}
}

func TestFormatting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	RunLogicTest(t, TestServerArgs{}, 0, testfile)
}
