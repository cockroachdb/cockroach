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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestInterleavedFastPathDeleteCheck(t *testing.T) {
	_, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})

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
		`childNoCascade`: `
CREATE TABLE IF NOT EXISTS childNoCascade(
  pid INT,
  id INT,
  PRIMARY KEY (pid, id),
  FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE NO ACTION
) INTERLEAVE IN PARENT parent(pid)
`,
		`grandchild`: `
CREATE TABLE grandchild(
    gid INT,
    pid INT,
    id INT,
    FOREIGN KEY (gid, pid) REFERENCES child(pid, id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY(gid, pid, id)
) INTERLEAVE IN PARENT child(gid, pid)
`,
	}

	drop := func(tables ...string) {
		// dropping has to be done in reverse so no drop causes a foreign key violation
		for i := len(tables) - 1; i >= 0; i-- {
			if _, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tables[i])); err != nil {
				t.Fatal(err)
			}
		}
	}

	// This function is to be called at the beginning of each sub-benchmark to set up the necessary tables.
	setup := func(tablesNeeded ...string) {
		drop(tablesNeeded...)
		for _, table := range tablesNeeded {
			log.Warningf(context.TODO(), "test: table %s", table)
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
			log.Infof(context.TODO(), "testing for tables %s", tableNames)
			setup(tableNames...)
			defer drop(tableNames...)

			tablesByID := make(map[sqlbase.ID]*TableDescriptor)
			pd := sqlbase.GetTableDescriptor(kvDB, "defaultdb", "parent")
			tablesByID[pd.ID] = pd

			for _, tableName := range tableNames {
				if tableName != "parent" {
					log.Warningf(context.TODO(), "getting descriptor for table %s", tableName)
					cd := sqlbase.GetTableDescriptor(kvDB, "defaultdb", tableName)
					tablesByID[cd.ID] = cd
				}
			}

			lookup := func(ctx context.Context, tableID sqlbase.ID) (sqlbase.TableLookup, error) {
				table, exists := tablesByID[tableID]
				if !exists {
					return sqlbase.TableLookup{}, errors.Errorf("Could not lookup table:%d", tableID)
				}
				return sqlbase.TableLookup{Table: table}, nil
			}

			fkTables, err := sqlbase.TablesNeededForFKs(
				context.TODO(),
				*pd,
				sqlbase.CheckDeletes,
				lookup,
				sqlbase.NoCheckPrivilege,
				nil, /* AnalyzeExprFunction */
			)
			if err != nil {
				t.Fatal(err)
			}
			tableIDs := make([]sqlbase.ID, 0)
			for id := range fkTables {
				tableIDs = append(tableIDs, id)
			}

			log.Infof(context.TODO(), "FAST PATH TEST for table %d: fkTables: %s", pd.ID, tableIDs)

			if canDeleteFastInterleaved(*pd, fkTables) != expected {
				t.Fatalf("canDeleteFastInterleaved returned %t, expected %t", !expected, expected)
			}
		}
	}

	t.Run("canDeleteFastInterleaved, one child", testCheck(true, "parent", "child"))
	t.Run("canDeleteFastInterleaved, two children", testCheck(true, "parent", "child", "sibling"))
	t.Run("canDeleteFastInterleaved, two children", testCheck(true, "parent", "child", "sibling", "grandchild"))
	t.Run("canDeleteFastInterleaved, two children", testCheck(true, "parent", "child", "grandchild"))
	//t.Run("canDeleteFastInterleaved, two children", testCheck(false, "parent", "childNoCascade")) // turning this on returns a "panic: missing table" triggered by trying to get the table descriptor for childNoCascade.
}
