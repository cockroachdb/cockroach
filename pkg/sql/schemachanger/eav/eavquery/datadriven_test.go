// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eavquery_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav/eavtest"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/datadriven"
)

func TestDataDriven(t *testing.T) {
	datadriven.Walk(t, testutils.TestDataPath(t, ""), func(t *testing.T, path string) {
		cmd := eavtest.NewDataDriven()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			return cmd.Cmd(t, d)
		})
	})
}
