// Copyright 2018 The Cockroach Authors.
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

package sql

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestInterleavedFastPathDeleteCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	tables := map[string]string{
		`parent`: `
CREATE TABLE IF NOT EXISTS parent(
  id INT PRIMARY KEY
)`,
		`child`: `
CREATE TABLE IF NOT EXISTS child(
  pid INT,
  id INT,
  PRIMARY KEY (pid, id),
  FOREIGN KEY(pid) REFERENCES parent(id) ON UPDATE CASCADE ON DELETE CASCADE
) INTERLEAVE IN PARENT parent(pid)
`,
		`sibling`: `
CREATE TABLE IF NOT EXISTS sibling(
  pid INT,
  id INT,
  PRIMARY KEY (pid, id),
  FOREIGN KEY(pid) REFERENCES parent(id) ON UPDATE CASCADE ON DELETE CASCADE
) INTERLEAVE IN PARENT parent(pid)
`,
		`grandchild`: `
CREATE TABLE IF NOT EXISTS grandchild(
    gid INT,
    pid INT,
    id INT,
    FOREIGN KEY (gid, pid) REFERENCES child(pid, id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY(gid, pid, id)
) INTERLEAVE IN PARENT child(gid, pid)
`,
		`external_ref`: `
CREATE TABLE IF NOT EXISTS external_ref(
	id INT,
	parent_id INT,
	child_id INT,
	FOREIGN KEY (parent_id, child_id) REFERENCES child(pid, id) ON DELETE CASCADE
)
`,
		`child_with_index`: `
CREATE TABLE IF NOT EXISTS child_with_index(
	pid INT,
	child_id INT,
	other_field STRING,
	PRIMARY KEY (pid, child_id),
	FOREIGN KEY (pid) REFERENCES parent(id),
	UNIQUE (other_field)
) INTERLEAVE IN PARENT parent(pid); CREATE INDEX ON child_with_index (other_field)
`,
	}

	drop := func(tables ...string) {
		for _, table := range tables {
			if _, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, table)); err != nil {
				t.Fatal(err)
			}
		}
	}

	// setup is called at the beginning of every sub-test
	setup := func(tablesNeeded ...string) {
		drop(tablesNeeded...)
		for _, table := range tablesNeeded {
			if _, ok := tables[table]; !ok {
				t.Fatal(errors.New("invalid table name for setup"))
			}
			if _, err := db.Exec(tables[table]); err != nil {
				t.Fatal(err)
			}
		}
	}

	testCheck := func(expected bool, tableNames ...string) func(*testing.T) {
		return func(t *testing.T) {
			setup(tableNames...)
			defer drop(tableNames...)

			tablesByID := make(map[sqlbase.ID]*ImmutableTableDescriptor)
			pd := sqlbase.GetImmutableTableDescriptor(kvDB, "defaultdb", "parent")
			tablesByID[pd.ID] = pd

			for _, tableName := range tableNames {
				if tableName != "parent" {
					cd := sqlbase.GetImmutableTableDescriptor(kvDB, "defaultdb", tableName)
					tablesByID[cd.ID] = cd
				}
			}

			lookup := func(ctx context.Context, tableID sqlbase.ID) (row.TableLookup, error) {
				table, exists := tablesByID[tableID]
				if !exists {
					return row.TableLookup{}, errors.Errorf("Could not lookup table:%d", tableID)
				}
				return row.TableLookup{Table: table}, nil
			}

			fkTables, err := row.TablesNeededForFKs(
				context.TODO(),
				pd,
				row.CheckDeletes,
				lookup,
				row.NoCheckPrivilege,
				nil, /* AnalyzeExprFunction */
			)
			if err != nil {
				t.Fatal(err)
			}

			if canDeleteFastInterleaved(pd, fkTables) != expected {
				t.Fatalf("canDeleteFastInterleaved returned %t, expected %t", !expected, expected)
			}
		}
	}

	t.Run("canDeleteFastInterleaved, one child", testCheck(true, "parent", "child"))
	t.Run("canDeleteFastInterleaved, two children", testCheck(true, "parent", "child", "sibling"))
	t.Run("canDeleteFastInterleaved, two children", testCheck(true, "parent", "child", "sibling", "grandchild"))
	t.Run("canDeleteFastInterleaved, two children", testCheck(true, "parent", "child", "grandchild"))
	t.Run("canDeleteFastInterleaved, one child, external ref", testCheck(false, "parent", "child", "external_ref"))
	t.Run("canDeleteFastInterleaved, one child with a secondary index", testCheck(false, "parent", "child_with_index"))
}

func TestInterleavedFastDeleteRestorable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	parentStmt := `
CREATE TABLE IF NOT EXISTS parent(
  id INT PRIMARY KEY
)`
	childStmt := `
CREATE TABLE IF NOT EXISTS child(
  pid INT,
  id INT,
  PRIMARY KEY (pid, id),
  FOREIGN KEY(pid) REFERENCES parent(id) ON UPDATE CASCADE ON DELETE CASCADE
) INTERLEAVE IN PARENT parent(pid)`

	if _, err := sqlDB.Exec(parentStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(childStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO parent SELECT generate_series(1,10)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO child(pid, id) SELECT parent.id, 1 from parent`); err != nil {
		t.Fatal(err)
	}
	var timestamp string
	if err := sqlDB.QueryRow("SELECT cluster_logical_timestamp()").Scan(&timestamp); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`DELETE FROM parent WHERE id <= 5`); err != nil {
		t.Fatal(err)
	}

	rows, err := sqlDB.Query(fmt.Sprintf(`SELECT * FROM parent AS OF SYSTEM TIME '%s'`, timestamp))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		count++
	}

	if count != 10 {
		t.Fatal(fmt.Sprintf("Expected 10 rows from AS OF SYSTEM TIME query, got %d", count))
	}

	rows, err = sqlDB.Query(fmt.Sprintf(`SELECT * FROM child AS OF SYSTEM TIME '%s'`, timestamp))
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	for rows.Next() {
		count++
	}

	if count != 10 {
		t.Fatal(fmt.Sprintf("Expected 10 rows from AS OF SYSTEM TIME query, got %d", count))
	}
}
