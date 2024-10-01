// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

func descriptorWithFKMutation(isDropped bool) string {
	dropState := ``
	dropTime := ``
	if isDropped {
		dropState = `"state": "DROP",`
		dropTime = `"dropTime": "1713940113794672911",`
	}
	return `'{
      "table": {
          "columns": [
              {
                  "id": 1,
                  "name": "i",
                  "type": {
                      "family": "IntFamily",
											"oid": 20,
                      "width": 64
                  }
              }
          ],` +
		dropTime +
		`"createAsOfTime": {
              "logical": 1,
              "wallTime": "1713940112376217646"
          },
          "families": [
              {
                  "columnIds": [
                      1
                  ],
                  "columnNames": [
                      "i"
                  ],
                  "name": "primary"
              }
          ],
          "formatVersion": 3,
          "id": 104,
          "modificationTime": {},
          "mutationJobs": [
              {
                  "jobId": "962952277419655169",
                  "mutationId": 1
              }
          ],
          "mutations": [
              {
                  "constraint": {
                      "check": {},
                      "constraintType": "FOREIGN_KEY",
                      "foreignKey": {
                          "constraintId": 2,
                          "name": "foo_foo_fk",
                          "onDelete": "CASCADE",
                          "onUpdate": "CASCADE",
                          "originColumnIds": [
                              1
                          ],
                          "originTableId": 104,
                          "referencedColumnIds": [
                              1
                          ],
                          "referencedTableId": 104,
                          "validity": "Validating"
                      },
                      "name": "foo_foo_fk",
                      "uniqueWithoutIndexConstraint": {}
                  },
                  "direction": "ADD",
                  "mutationId": 1,
                  "state": "DELETE_ONLY"
              }
          ],
          "name": "foo",
          "nextColumnId": 2,
          "nextConstraintId": 3,
          "nextFamilyId": 1,
          "nextIndexId": 2,
          "nextMutationId": 2,
          "parentId": 183,
          "primaryIndex": {
              "constraintId": 1,
              "createdAtNanos": "1713940112106985000",
              "encodingType": 1,
              "foreignKey": {},
              "geoConfig": {},
              "id": 1,
              "interleave": {},
              "keyColumnDirections": [
                  "DESC"
              ],
              "keyColumnIds": [
                  1
              ],
              "keyColumnNames": [
                  "i"
              ],
              "name": "table_w3_143_pkey",
              "partitioning": {},
              "sharded": {},
              "unique": true,
              "version": 4
          },
          "privileges": {
              "ownerProto": "roachprod",
              "users": [
                  {
                      "privileges": "2",
                      "userProto": "admin",
                      "withGrantOption": "2"
                  },
                  {
                      "privileges": "2",
                      "userProto": "root",
                      "withGrantOption": "2"
                  }
              ],
              "version": 3
          },
          "replacementOf": {
              "time": {}
          },` +
		dropState +
		`"unexposedParentSchemaId": 381,
          "version": "2"
      }
  }'`
}

// This test doctoring a secure cluster.
func TestDoctorCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	//
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

// TestDoctorClusterBroken tests that debug doctor examine will pick up a multitude of issues on a corrupt descriptor.
func TestDoctorClusterBroken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewCLITest(TestCLIParams{T: t, DisableAutoStats: true})
	defer c.Cleanup()

	desc := fmt.Sprintf("SELECT crdb_internal.unsafe_upsert_descriptor('foo'::regclass::oid::int,"+
		"crdb_internal.json_to_pb('cockroach.sql.sqlbase.Descriptor', %s::jsonb), true)", descriptorWithFKMutation(false /* isDropped */))

	// Introduce a descriptor with an attached job mutation (along with other issues). We want to ensure that the number of
	// jobs created is deterministic (auto table stats will be collected due to the "schema change" on foo); therefore,
	// we should disable automatic stats collection and instead create our own.
	c.RunWithArgs([]string{"sql", "-e", strings.Join([]string{
		"CREATE TABLE foo (i INT)",
		desc,
		"CREATE STATISTICS foo_stats FROM foo",
		"SELECT pg_catalog.pg_sleep(1)",
	}, ";\n"),
	})

	t.Run("examine", func(t *testing.T) {
		out, err := c.RunWithCapture("debug doctor examine cluster")
		if err != nil {
			t.Fatal(err)
		}

		// Using datadriven allows TESTFLAGS=-rewrite.
		datadriven.RunTest(t, datapathutils.TestDataPath(t, "doctor", "test_examine_cluster_jobs"), func(t *testing.T, td *datadriven.TestData) string {
			return out
		})
	})
}

// TestDoctorClusterDropped tests that debug doctor examine will avoid validating dropped descriptors.
func TestDoctorClusterDropped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewCLITest(TestCLIParams{T: t})
	defer c.Cleanup()

	desc := fmt.Sprintf("SELECT crdb_internal.unsafe_upsert_descriptor('foo'::regclass::oid::int,"+
		"crdb_internal.json_to_pb('cockroach.sql.sqlbase.Descriptor', %s::jsonb), true)", descriptorWithFKMutation(true /* isDropped */))
	// Introduce a dropped descriptor with an attached job mutation (along with other issues).
	c.RunWithArgs([]string{"sql", "-e", strings.Join([]string{
		"CREATE TABLE foo (i INT)",
		desc,
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
		datadriven.RunTest(t, datapathutils.TestDataPath(t, "doctor", "test_examine_cluster_dropped"), func(t *testing.T, td *datadriven.TestData) string {
			return out
		})
	})
}

// TestDoctorZipDir tests the operation of zip over secure clusters.
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

	// Regression test (for #104347) to ensure that quoted table names get properly parsed in system.namespace.
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
