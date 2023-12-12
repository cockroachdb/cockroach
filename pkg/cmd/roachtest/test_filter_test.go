// Copyright 2023 The Cockroach Authors.
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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
)

func TestMakeTestFilterTagCompatibility(t *testing.T) {
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "test-filter-tag-compat"), func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "make-test-filter" {
			d.Fatalf(t, "invalid command %q", d.Cmd)
		}
		args := strings.Split(d.Input, " ")
		f, err := makeTestFilter(args)
		if err != nil {
			return fmt.Sprintf("error: %v", err)
		}
		return fmt.Sprintf("regexp: %s\nsuite: %s", f.Name, f.Suite)
	})
}
