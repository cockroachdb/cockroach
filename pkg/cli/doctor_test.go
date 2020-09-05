// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// This test doctoring a secure cluster.
func TestDoctorCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	// Introduce a corruption in the descriptor table by adding a table and
	// removing its parent.
	c.RunWithArgs([]string{"sql", "-e", strings.Join([]string{
		"CREATE TABLE to_drop (id INT)",
		"DROP TABLE to_drop",
		"CREATE TABLE foo (id INT)",
		"INSERT INTO system.users VALUES ('node', NULL, true)",
		"GRANT node TO root",
		"DELETE FROM system.namespace WHERE name = 'foo'",
	}, ";\n"),
	})

	out, err := c.RunWithCapture("debug doctor cluster")
	if err != nil {
		t.Fatal(err)
	}

	// Using datadriven allows TESTFLAGS=-rewrite.
	datadriven.RunTest(t, "testdata/doctor/testcluster", func(t *testing.T, td *datadriven.TestData) string {
		return out
	})
}

// This test the operation of zip over secure clusters.
func TestDoctorZipDir(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := newCLITest(cliTestParams{t: t, noServer: true})
	defer c.cleanup()

	out, err := c.RunWithCapture("debug doctor zipdir testdata/doctor/debugzip")
	if err != nil {
		t.Fatal(err)
	}

	// Using datadriven allows TESTFLAGS=-rewrite.
	datadriven.RunTest(t, "testdata/doctor/testzipdir", func(t *testing.T, td *datadriven.TestData) string {
		return out
	})
}
