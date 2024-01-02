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

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// This test doctoring a secure cluster.
func TestDoctorCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewCLITest(TestCLIParams{T: t})
	defer c.Cleanup()

	// Introduce a corruption in the descriptor table by adding a table and
	// removing its parent.
	c.RunWithArgs([]string{"sql", "-e", strings.Join([]string{
		"CREATE TABLE to_drop (id INT)",
		"DROP TABLE to_drop",
		"CREATE TABLE foo (id INT)",
		"INSERT INTO system.users VALUES ('node', NULL, true, 3)",
		"GRANT node TO root",
		"DELETE FROM system.namespace WHERE name = 'foo'",
		"SELECT pg_catalog.pg_sleep(1)",
	}, ";\n"),
	})

	t.Run("examine", func(t *testing.T) {
		out, err := c.RunWithCapture("debug doctor examine cluster")
		if err != nil {
			t.Fatal(err)
		}

		// Using datadriven allows TESTFLAGS=-rewrite.
		datadriven.RunTest(t, datapathutils.TestDataPath(t, "doctor", "test_examine_cluster"), func(t *testing.T, td *datadriven.TestData) string {
			return out
		})
	})
}

// This test the operation of zip over secure clusters.
func TestDoctorZipDir(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewCLITest(TestCLIParams{T: t, NoServer: true})
	defer c.Cleanup()

	t.Run("examine", func(t *testing.T) {
		out, err := c.RunWithCapture("debug doctor examine zipdir testdata/doctor/debugzip 21.1-52")
		if err != nil {
			t.Fatal(err)
		}

		// Using datadriven allows TESTFLAGS=-rewrite.
		datadriven.RunTest(t, datapathutils.TestDataPath(t, "doctor", "test_examine_zipdir"), func(t *testing.T, td *datadriven.TestData) string {
			return out
		})
	})

	t.Run("examine", func(t *testing.T) {
		out, err := c.RunWithCapture("debug doctor examine zipdir testdata/doctor/debugzip-with-quotes")
		if err != nil {
			t.Fatal(err)
		}

		// Using datadriven allows TESTFLAGS=-rewrite.
		datadriven.RunTest(t, datapathutils.TestDataPath(t, "doctor", "test_examine_zipdir_with_quotes"), func(t *testing.T, td *datadriven.TestData) string {
			return out
		})
	})

	t.Run("recreate", func(t *testing.T) {
		out, err := c.RunWithCapture("debug doctor recreate zipdir testdata/doctor/debugzip")
		if err != nil {
			t.Fatal(err)
		}

		// Using datadriven allows TESTFLAGS=-rewrite.
		datadriven.RunTest(t, datapathutils.TestDataPath(t, "doctor", "test_recreate_zipdir"), func(t *testing.T, td *datadriven.TestData) string {
			return out
		})
	})

	t.Run("recreate-json", func(t *testing.T) {
		out, err := c.RunWithCapture("debug doctor recreate zipdir testdata/doctor/debugzip-json")
		if err != nil {
			t.Fatal(err)
		}

		// Using datadriven allows TESTFLAGS=-rewrite.
		datadriven.RunTest(t, datapathutils.TestDataPath(t, "doctor", "test_recreate_zipdir-json"), func(t *testing.T, td *datadriven.TestData) string {
			return out
		})
	})

	t.Run("deprecated doctor zipdir with verbose", func(t *testing.T) {
		out, err := c.RunWithCapture("debug doctor zipdir testdata/doctor/debugzip 21.11-52 --verbose")
		if err != nil {
			t.Fatal(err)
		}

		// Using datadriven allows TESTFLAGS=-rewrite.
		datadriven.RunTest(t, datapathutils.TestDataPath(t, "doctor", "test_examine_zipdir_verbose"), func(t *testing.T, td *datadriven.TestData) string {
			return out
		})
	})
}
