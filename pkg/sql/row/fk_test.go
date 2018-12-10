// Copyright 2017 The Cockroach Authors.
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

package row

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

type testTables struct {
	nextID       ID
	tablesByID   map[ID]*sqlbase.ImmutableTableDescriptor
	tablesByName map[string]*sqlbase.ImmutableTableDescriptor
}

func (t *testTables) createTestTable(name string) ID {
	table := sqlbase.NewImmutableTableDescriptor(sqlbase.TableDescriptor{
		Name:        name,
		ID:          t.nextID,
		NextIndexID: sqlbase.IndexID(1), // This must be 1 to avoid clashing with a primary index.
	})
	t.tablesByID[table.ID] = table
	t.tablesByName[table.Name] = table
	t.nextID++
	return table.ID
}

func (t *testTables) createForeignKeyReference(
	referencingID ID,
	referencedID ID,
	onDelete sqlbase.ForeignKeyReference_Action,
	onUpdate sqlbase.ForeignKeyReference_Action,
) error {
	// Get the tables
	referencing, exists := t.tablesByID[referencingID]
	if !exists {
		return errors.Errorf("Can't find table with ID:%d", referencingID)
	}
	referenced, exists := t.tablesByID[referencedID]
	if !exists {
		return errors.Errorf("Can't find table with ID:%d", referencedID)
	}
	// Create an index on both tables.
	referencedIndexID := referenced.NextIndexID
	referencingIndexID := referencing.NextIndexID
	referencedIndex := sqlbase.IndexDescriptor{
		ID: referencedIndexID,
		ReferencedBy: []sqlbase.ForeignKeyReference{
			{
				Table: referencingID,
				Index: referencingIndexID,
			},
		},
	}
	referenced.Indexes = append(referenced.Indexes, referencedIndex)

	referencingIndex := sqlbase.IndexDescriptor{
		ID: referencingIndexID,
		ForeignKey: sqlbase.ForeignKeyReference{
			Table:    referencedID,
			OnDelete: onDelete,
			OnUpdate: onUpdate,
			Index:    referencedIndexID,
		},
	}
	referencing.Indexes = append(referencing.Indexes, referencingIndex)

	referenced.NextIndexID++
	referencing.NextIndexID++
	return nil
}

// TestTablesNeededForFKs creates an artificial set of tables to test the graph
// walking algorithm used in the function.
func TestTablesNeededForFKs(t *testing.T) {
	tables := testTables{
		nextID:       ID(1),
		tablesByID:   make(map[ID]*sqlbase.ImmutableTableDescriptor),
		tablesByName: make(map[string]*sqlbase.ImmutableTableDescriptor),
	}

	// First setup the table we will be testing against.
	xID := tables.createTestTable("X")

	expectedInsertIDs := []ID{xID}
	expectedUpdateIDs := []ID{xID}
	expectedDeleteIDs := []ID{xID}

	// For all possible combinations of relationships for foreign keys, create a
	// table that X references, and one that references X.
	for deleteNum, deleteName := range sqlbase.ForeignKeyReference_Action_name {
		for updateNum, updateName := range sqlbase.ForeignKeyReference_Action_name {
			subName := fmt.Sprintf("OnDelete%s OnUpdate%s", deleteName, updateName)
			referencedByX := tables.createTestTable(fmt.Sprintf("X Referenced - %s", subName))
			if err := tables.createForeignKeyReference(
				xID, referencedByX, sqlbase.ForeignKeyReference_Action(deleteNum), sqlbase.ForeignKeyReference_Action(updateNum),
			); err != nil {
				t.Fatalf("could not add index: %s", err)
			}

			referencingX := tables.createTestTable(fmt.Sprintf("Referencing X - %s", subName))
			if err := tables.createForeignKeyReference(
				referencingX, xID, sqlbase.ForeignKeyReference_Action(deleteNum), sqlbase.ForeignKeyReference_Action(updateNum),
			); err != nil {
				t.Fatalf("could not add index: %s", err)
			}

			expectedInsertIDs = append(expectedInsertIDs, referencedByX)
			expectedUpdateIDs = append(expectedUpdateIDs, referencedByX)
			expectedUpdateIDs = append(expectedUpdateIDs, referencingX)
			expectedDeleteIDs = append(expectedDeleteIDs, referencingX)

			// To go even further, create another set of tables for all possible
			// foreign key relationships that reference the table that is referencing
			// X. This will ensure that we bound the tree walking algorithm correctly.
			for deleteNum2, deleteName2 := range sqlbase.ForeignKeyReference_Action_name {
				for updateNum2, updateName2 := range sqlbase.ForeignKeyReference_Action_name {
					//if deleteNum2 != int32(ForeignKeyReference_CASCADE) || updateNum2 != int32(ForeignKeyReference_CASCADE) {
					//	continue
					//}
					subName2 := fmt.Sprintf("Referencing %d - OnDelete%s OnUpdated%s", referencingX, deleteName2, updateName2)
					referencing2 := tables.createTestTable(subName2)
					if err := tables.createForeignKeyReference(
						referencing2, referencingX, sqlbase.ForeignKeyReference_Action(deleteNum2), sqlbase.ForeignKeyReference_Action(updateNum2),
					); err != nil {
						t.Fatalf("could not add index: %s", err)
					}

					// Only fetch the next level of tables if a cascade can occur through
					// the first level.
					if deleteNum == int32(sqlbase.ForeignKeyReference_CASCADE) ||
						deleteNum == int32(sqlbase.ForeignKeyReference_SET_DEFAULT) ||
						deleteNum == int32(sqlbase.ForeignKeyReference_SET_NULL) {
						expectedDeleteIDs = append(expectedDeleteIDs, referencing2)
					}
					if updateNum == int32(sqlbase.ForeignKeyReference_CASCADE) ||
						updateNum == int32(sqlbase.ForeignKeyReference_SET_DEFAULT) ||
						updateNum == int32(sqlbase.ForeignKeyReference_SET_NULL) {
						expectedUpdateIDs = append(expectedUpdateIDs, referencing2)
					}
				}
			}
		}
	}

	sort.Slice(expectedInsertIDs, func(i, j int) bool { return expectedInsertIDs[i] < expectedInsertIDs[j] })
	sort.Slice(expectedUpdateIDs, func(i, j int) bool { return expectedUpdateIDs[i] < expectedUpdateIDs[j] })
	sort.Slice(expectedDeleteIDs, func(i, j int) bool { return expectedDeleteIDs[i] < expectedDeleteIDs[j] })

	xDesc, exists := tables.tablesByID[xID]
	if !exists {
		t.Fatalf("Could not find table:%d", xID)
	}

	lookup := func(ctx context.Context, tableID ID) (TableLookup, error) {
		table, exists := tables.tablesByID[tableID]
		if !exists {
			return TableLookup{}, errors.Errorf("Could not lookup table:%d", tableID)
		}
		return TableLookup{Table: table}, nil
	}

	test := func(t *testing.T, usage FKCheck, expectedIDs []ID) {
		tableLookups, err := TablesNeededForFKs(
			context.TODO(),
			xDesc,
			usage,
			lookup,
			NoCheckPrivilege,
			nil, /* AnalyzeExprFunction */
		)
		if err != nil {
			t.Fatal(err)
		}
		var actualIDs []ID
		for id := range tableLookups {
			actualIDs = append(actualIDs, id)
		}
		sort.Slice(actualIDs, func(i, j int) bool { return actualIDs[i] < actualIDs[j] })
		if a, e := actualIDs, expectedIDs; !reflect.DeepEqual(a, e) {
			t.Errorf("insert's expected table IDs did not match actual IDs diff:\n %v", pretty.Diff(e, a))
		}
	}

	t.Run("Inserts", func(t *testing.T) {
		test(t, CheckInserts, expectedInsertIDs)
	})
	t.Run("Updates", func(t *testing.T) {
		test(t, CheckUpdates, expectedUpdateIDs)
	})
	t.Run("Deletes", func(t *testing.T) {
		test(t, CheckDeletes, expectedDeleteIDs)
	})
}

// BenchmarkMultiRowFKChecks performs several benchmarks that pertain to operations involving foreign keys and cascades.
func BenchmarkMultiRowFKChecks(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	defer log.Scope(b).Close(b)

	// Throughout the course of testing there are four tables that are set up at the beginning of each sub-benchmark and
	// torn down at the end of each sub-benchmark.
	// `childFK` has a foreign key that references `parentFK`.
	fkTables := map[string]string{
		`parentFK`: `
CREATE TABLE IF NOT EXISTS parentFK(
  foo INT PRIMARY KEY,
  bar INT
)`,
		`childFK`: `
CREATE TABLE IF NOT EXISTS childFK(
  baz INT,
  foo INT,
  FOREIGN KEY(foo) REFERENCES parentFK(foo) ON UPDATE CASCADE ON DELETE CASCADE
)
`,
		// `parentNoFK` and `childNoFK` are the same as `parentFK` and `childFK` but `childNoFK` has no foreign key reference
		// to `parentNoFK`
		`parentNoFK`: `
CREATE TABLE IF NOT EXISTS parentNoFK(
  foo INT PRIMARY KEY,
  bar INT
)
`,
		`childNoFK`: `
CREATE TABLE IF NOT EXISTS childNoFK(
  baz INT,
  foo INT
)`,
		`parentInterleaved`: `
CREATE TABLE IF NOT EXISTS parentInterleaved(
	foo INT PRIMARY KEY,
	bar int
)`,
		`childInterleaved`: `
CREATE TABLE IF NOT EXISTS childInterleaved(
	baz INT,
	foo INT,
	PRIMARY KEY(foo, baz),
	FOREIGN KEY(foo) REFERENCES parentInterleaved(foo) ON UPDATE CASCADE ON DELETE CASCADE
) INTERLEAVE IN PARENT parentInterleaved (foo)`,
		`siblingInterleaved`: `
CREATE TABLE IF NOT EXISTS siblingInterleaved(
	baz INT,
	foo INT,
	PRIMARY KEY(foo, baz),
	FOREIGN KEY(foo) REFERENCES parentInterleaved(foo) ON UPDATE CASCADE ON DELETE CASCADE
) INTERLEAVE IN PARENT parentInterleaved (foo)`,
		`grandchildInterleaved`: `
CREATE TABLE IF NOT EXISTS grandchildInterleaved(
	bar INT,
	foo INT,
	baz INT,
	PRIMARY KEY(foo, baz, bar),
	FOREIGN KEY(foo, baz) REFERENCES childInterleaved(foo, baz) ON UPDATE CASCADE ON DELETE CASCADE
) INTERLEAVE IN PARENT childInterleaved (foo, baz)`,
		// `self_referential` has a foreign key reference to itself (parent-child relationship) with
		// 		cascading updates and deletes
		// `self_referential_noFK` has the same schema
		// `self_referential_setnull` has an identical schema to `self_referential` except that instead of cascading
		// 		on delete, it sets the reference field to null.
		`self_referential`: `
CREATE TABLE IF NOT EXISTS self_referential(
	id INT PRIMARY KEY,
	pid INT,
	FOREIGN KEY(pid) REFERENCES self_referential(id) ON UPDATE CASCADE ON DELETE CASCADE
)`,
		`self_referential_noFK`: `
CREATE TABLE IF NOT EXISTS self_referential_noFK(
	id INT PRIMARY KEY,
	pid INT
)`,
		`self_referential_setnull`: `
CREATE TABLE IF NOT EXISTS self_referential_setnull(
	id INT PRIMARY KEY,
	pid INT,
	FOREIGN KEY(pid) REFERENCES self_referential_setnull(id) ON UPDATE CASCADE ON DELETE SET NULL
)`,
	}
	_, db, _ := serverutils.StartServer(b, base.TestServerArgs{})

	// This function tears down all the tables and is meant to be called at the beginning and  end of each sub-benchmark.
	drop := func() {
		// dropping has to be done in reverse so no drop causes a foreign key violation
		for tableName := range fkTables {
			if _, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, tableName)); err != nil {
				b.Fatal(err)
			}
		}
	}

	// This function is to be called at the beginning of each sub-benchmark to set up the necessary tables.
	setup := func(tablesNeeded []string) {
		drop()
		for _, t := range tablesNeeded {
			if _, ok := fkTables[t]; !ok {
				b.Fatal(errors.New("invalid table name for setup"))
			}
			if _, err := db.Exec(fkTables[t]); err != nil {
				b.Fatal(err)
			}
		}
	}

	// The following sub-benchmarks are for the parentFK/childFK and parentNoFK/childNoFK tables.
	// The insertRows and deleteRows sub-benchmarks of each kind measures insert performance and delete performance (respectively)
	// of the following cases:
	//     * {insert,delete}Rows_IdenticalFK: All rows in child reference the same row in parent
	//     * {insert,delete}Rows_NoFK: Uses parentNoFK/childNoFK tables, no foreign key refs
	//     * {insert,delete}Rows_UniqueFKs: All rows in child reference a distinct row in parent
	const numFKRows = 10000
	b.Run("insertRows_IdenticalFK", func(b *testing.B) {
		setup([]string{`parentFK`, `childFK`})
		if _, err := db.Exec(`INSERT INTO parentFK(foo) VALUES(1)`); err != nil {
			b.Fatal(err)
		}
		defer drop()
		b.ResetTimer()
		var run bytes.Buffer

		run.WriteString(`INSERT INTO childFK(baz, foo) VALUES `)

		for i := 1; i <= numFKRows; i++ {
			run.WriteString(fmt.Sprintf("(%d, 1)", i))
			if i != numFKRows {
				run.WriteString(", ")
			}
		}

		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})
	b.Run("deleteRows_IdenticalFK", func(b *testing.B) {
		setup([]string{`parentFK`, `childFK`})
		if _, err := db.Exec(`INSERT INTO parentFK(foo) VALUES(1)`); err != nil {
			b.Fatal(err)
		}
		defer drop()
		var run bytes.Buffer

		run.WriteString(`INSERT INTO childFK(baz, foo) VALUES `)

		for i := 1; i <= numFKRows; i++ {
			run.WriteString(fmt.Sprintf("(%d, 1)", i))
			if i != numFKRows {
				run.WriteString(", ")
			}
		}

		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		if _, err := db.Exec(`DELETE from childFK`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	b.Run("insertRows_UniqueFKs", func(b *testing.B) {
		setup([]string{`parentFK`, `childFK`})
		for i := 1; i <= numFKRows; i++ {
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO parentFK(foo) VALUES(%d)`, i)); err != nil {
				b.Fatal(err)
			}
		}
		defer drop()
		b.ResetTimer()
		var run bytes.Buffer

		run.WriteString(`INSERT INTO childFK(baz, foo) VALUES `)

		for i := 1; i <= numFKRows; i++ {
			run.WriteString(fmt.Sprintf("(%d, %d)", i, i))
			if i != numFKRows {
				run.WriteString(", ")
			}
		}

		b.ResetTimer()
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	b.Run("deleteRows_UniqueFKs", func(b *testing.B) {
		setup([]string{`parentFK`, `childFK`})
		for i := 1; i <= numFKRows; i++ {
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO parentFK(foo) VALUES(%d)`, i)); err != nil {
				b.Fatal(err)
			}
		}
		defer drop()
		b.ResetTimer()
		var run bytes.Buffer

		run.WriteString(`INSERT INTO childFK(baz, foo) VALUES `)

		for i := 1; i <= numFKRows; i++ {
			run.WriteString(fmt.Sprintf("(%d, %d)", i, i))
			if i != numFKRows {
				run.WriteString(", ")
			}
		}
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		if _, err := db.Exec(`DELETE FROM childFK`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	b.Run("insertRows_NoFK", func(b *testing.B) {
		setup([]string{`parentNoFK`, `childNoFK`})
		if _, err := db.Exec(`INSERT INTO parentNoFK(foo) VALUES(1)`); err != nil {
			b.Fatal(err)
		}
		defer drop()
		b.ResetTimer()
		var run bytes.Buffer
		run.WriteString(`INSERT INTO childNoFK(baz, foo) VALUES `)

		for i := 1; i <= numFKRows; i++ {
			run.WriteString(fmt.Sprintf("(%d, 1)", i))
			if i != numFKRows {
				run.WriteString(", ")
			}
		}
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
	})
	b.Run("deleteRows_NoFK", func(b *testing.B) {
		setup([]string{`parentNoFK`, `childNoFK`})
		if _, err := db.Exec(`INSERT INTO parentNoFK(foo) VALUES(1)`); err != nil {
			b.Fatal(err)
		}
		defer drop()
		var run bytes.Buffer
		run.WriteString(`INSERT INTO childNoFK(baz, foo) VALUES `)

		for i := 1; i <= numFKRows; i++ {
			run.WriteString(fmt.Sprintf("(%d, 1)", i))
			if i != numFKRows {
				run.WriteString(", ")
			}
		}
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		if _, err := db.Exec(`DELETE FROM childNoFK`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	const numFKRowsMultipleRef = 1000
	const refsPerRow = 10
	b.Run("insertRows_multiple_refs", func(b *testing.B) {
		setup([]string{`parentFK`, `childFK`})
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO parentFK(foo) VALUES(%d)`, i)); err != nil {
				b.Fatal(err)
			}
		}
		defer drop()
		var run bytes.Buffer
		b.ResetTimer()
		run.WriteString(`INSERT INTO childFK(baz, foo) VALUES `)
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			for j := 1; j <= refsPerRow; j++ {
				run.WriteString(fmt.Sprintf("(%d, %d)", j, i))
				if i != numFKRowsMultipleRef || j != refsPerRow {
					run.WriteString(", ")
				}
			}
		}
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})
	b.Run("deleteRows_multiple_refs", func(b *testing.B) {
		setup([]string{`parentFK`, `childFK`})
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO parentFK(foo) VALUES(%d)`, i)); err != nil {
				b.Fatal(err)
			}
		}
		defer drop()
		var run bytes.Buffer
		run.WriteString(`INSERT INTO childFK(baz, foo) VALUES `)
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			for j := 1; j <= refsPerRow; j++ {
				run.WriteString(fmt.Sprintf("(%d, %d)", j, i))
				if i != numFKRowsMultipleRef || j != refsPerRow {
					run.WriteString(", ")
				}
			}
		}
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		if _, err := db.Exec(`DELETE FROM childFK`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})
	b.Run("insertRows_multiple_refs_noFK", func(b *testing.B) {
		setup([]string{`parentNoFK`, `childNoFK`})
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO parentNoFK(foo) VALUES(%d)`, i)); err != nil {
				b.Fatal(err)
			}
		}
		defer drop()
		var run bytes.Buffer
		b.ResetTimer()
		run.WriteString(`INSERT INTO childNoFK(baz, foo) VALUES `)
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			for j := 1; j <= refsPerRow; j++ {
				run.WriteString(fmt.Sprintf("(%d, %d)", j, i))
				if i != numFKRowsMultipleRef || j != refsPerRow {
					run.WriteString(", ")
				}
			}
		}
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	b.Run("deleteRows_multiple_refs_No_FK", func(b *testing.B) {
		setup([]string{`parentNoFK`, `childNoFK`})
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO parentNoFK(foo) VALUES(%d)`, i)); err != nil {
				b.Fatal(err)
			}
		}
		defer drop()
		var run bytes.Buffer
		run.WriteString(`INSERT INTO childNoFK(baz, foo) VALUES `)
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			for j := 1; j <= refsPerRow; j++ {
				run.WriteString(fmt.Sprintf("(%d, %d)", j, i))
				if i != numFKRowsMultipleRef || j != refsPerRow {
					run.WriteString(", ")
				}
			}
		}
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		if _, err := db.Exec(`DELETE FROM childNoFK`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	// Inserts and deletes are tested for interleaved tables
	b.Run("insertRows_interleaved", func(b *testing.B) {
		setup([]string{`parentInterleaved`, `childInterleaved`})
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO parentInterleaved(foo) VALUES(%d)`, i)); err != nil {
				b.Fatal(err)
			}
		}
		defer drop()
		var run bytes.Buffer
		b.ResetTimer()
		run.WriteString(`INSERT INTO childInterleaved(baz, foo) VALUES `)
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			for j := 1; j <= refsPerRow; j++ {
				run.WriteString(fmt.Sprintf("(%d, %d)", j, i))
				if i != numFKRowsMultipleRef || j != refsPerRow {
					run.WriteString(", ")
				}
			}
		}
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	b.Run("deleteRows_interleaved", func(b *testing.B) {
		setup([]string{`parentInterleaved`, `childInterleaved`})
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO parentInterleaved(foo) VALUES(%d)`, i)); err != nil {
				b.Fatal(err)
			}
		}
		defer drop()
		var run bytes.Buffer
		run.WriteString(`INSERT INTO childInterleaved(baz, foo) VALUES `)
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			for j := 1; j <= refsPerRow; j++ {
				run.WriteString(fmt.Sprintf("(%d, %d)", j, i))
				if i != numFKRowsMultipleRef || j != refsPerRow {
					run.WriteString(", ")
				}
			}
		}
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		if _, err := db.Exec(`DELETE FROM childInterleaved`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	// This tests the performance of deleting rows from the parent table
	b.Run("deleteRowsFromParent_interleaved", func(b *testing.B) {
		setup([]string{`parentInterleaved`, `childInterleaved`})
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO parentInterleaved(foo) VALUES(%d)`, i)); err != nil {
				b.Fatal(err)
			}
		}
		defer drop()
		var run bytes.Buffer
		run.WriteString(`INSERT INTO childInterleaved(baz, foo) VALUES `)
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			for j := 1; j <= refsPerRow; j++ {
				run.WriteString(fmt.Sprintf("(%d, %d)", j, i))
				if i != numFKRowsMultipleRef || j != refsPerRow {
					run.WriteString(", ")
				}
			}
		}
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		if _, err := db.Exec(`DELETE FROM parentInterleaved`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	// This tests the performance of deleting rows from the parent table when there are two interleaved tables
	b.Run("deleteRowsFromParent_interleaved_sibling", func(b *testing.B) {
		setup([]string{`parentInterleaved`, `childInterleaved`, `siblingInterleaved`})
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO parentInterleaved(foo) VALUES(%d)`, i)); err != nil {
				b.Fatal(err)
			}
		}
		defer drop()
		var run, runSibling bytes.Buffer
		run.WriteString(`INSERT INTO childInterleaved(baz, foo) VALUES `)
		runSibling.WriteString(`INSERT INTO siblingInterleaved(baz, foo) VALUES `)
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			for j := 1; j <= refsPerRow; j++ {
				run.WriteString(fmt.Sprintf("(%d, %d)", j, i))
				runSibling.WriteString(fmt.Sprintf("(%d, %d)", j, i))
				if i != numFKRowsMultipleRef || j != refsPerRow {
					run.WriteString(", ")
					runSibling.WriteString(", ")
				}
			}
		}
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		statementSibling := runSibling.String()
		if _, err := db.Exec(statementSibling); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		if _, err := db.Exec(`DELETE FROM parentInterleaved`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	// This tests the performance of deleting rows from the parent table when there is an interleaved table
	// and another table further interleaved in it.
	b.Run("deleteRowsFromParent_interleaved_grandchild", func(b *testing.B) {
		setup([]string{`parentInterleaved`, `childInterleaved`, `grandchildInterleaved`})
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO parentInterleaved(foo) VALUES(%d)`, i)); err != nil {
				b.Fatal(err)
			}
		}
		defer drop()
		var run, runGrandchild bytes.Buffer
		run.WriteString(`INSERT INTO childInterleaved(baz, foo) VALUES `)
		runGrandchild.WriteString(`INSERT INTO grandChildInterleaved(foo, baz, bar) VALUES `)
		for i := 1; i <= numFKRowsMultipleRef; i++ {
			for j := 1; j <= refsPerRow; j++ {
				run.WriteString(fmt.Sprintf("(%d, %d)", j, i))
				runGrandchild.WriteString(fmt.Sprintf("(%d, %d, 1)", i, j))
				if i != numFKRowsMultipleRef || j != refsPerRow {
					run.WriteString(", ")
					runGrandchild.WriteString(", ")
				}
			}
		}
		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		statementGrandchild := runGrandchild.String()
		if _, err := db.Exec(statementGrandchild); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		if _, err := db.Exec(`DELETE FROM parentInterleaved`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	// For the self-referential table benchmarks, `numSRRows` rows are inserted and there is again a contrast between
	// rows with foreign key references and those without.
	// There are several different cases:
	// Cascade: casacading deletes
	// No_FK: no foreign key references
	// SetNull: ... ON DELETE SET NULL foreign key reference
	// Within each of these three categories, there are two cases:
	// Chain: row i references row i-1, chaining until row 1
	// ManyChildren: row 2..numSRRows all reference row 1
	const numSRRows = 10000
	b.Run("SelfReferential_Cascade_FK_Chain_Delete", func(b *testing.B) {
		setup([]string{`self_referential`})
		defer drop()
		if _, err := db.Exec(`INSERT INTO self_referential(id) VALUES (1)`); err != nil {
			b.Fatal(err)
		}

		for i := 2; i <= numSRRows; i++ {
			insert := fmt.Sprintf(`INSERT INTO self_referential(id, pid) VALUES (%d, %d)`, i, i-1)
			if _, err := db.Exec(insert); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()

		if _, err := db.Exec(`DELETE FROM self_referential`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	b.Run("SelfReferential_Cascade_FK_ManyChildren_Delete", func(b *testing.B) {
		setup([]string{`self_referential`})
		defer drop()
		if _, err := db.Exec(`INSERT INTO self_referential(id) VALUES (1)`); err != nil {
			b.Fatal(err)
		}

		for i := 2; i <= numSRRows; i++ {
			insert := fmt.Sprintf(`INSERT INTO self_referential(id, pid) VALUES (%d, 1)`, i)
			if _, err := db.Exec(insert); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()
		if _, err := db.Exec(`DELETE FROM self_referential`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	b.Run("SelfReferential_No_FK_Chain_Delete", func(b *testing.B) {
		setup([]string{`self_referential_noFK`})
		defer drop()
		var insert bytes.Buffer
		insert.WriteString(`INSERT INTO self_referential_noFK(id) VALUES `)
		for i := 1; i <= numSRRows; i++ {
			insert.WriteString(fmt.Sprintf(`(%d)`, i))
			if i != numSRRows {
				insert.WriteString(`, `)
			}
		}

		if _, err := db.Exec(insert.String()); err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()

		if _, err := db.Exec(`DELETE FROM self_referential_noFK`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})
	b.Run("SelfReferential_No_FK_ManyChildren_Delete", func(b *testing.B) {
		setup([]string{`self_referential_noFK`})
		defer drop()
		if _, err := db.Exec(`INSERT INTO self_referential_noFK(id) VALUES (1)`); err != nil {
			b.Fatal(err)
		}

		for i := 2; i <= numSRRows; i++ {
			insert := fmt.Sprintf(`INSERT INTO self_referential_noFK(id, pid) VALUES (%d, 1)`, i)
			if _, err := db.Exec(insert); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()
		if _, err := db.Exec(`DELETE FROM self_referential_noFK`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})
	b.Run("SelfReferential_SetNull_FK_Chain_Delete", func(b *testing.B) {
		setup([]string{`self_referential_setnull`})
		defer drop()
		run3 := `INSERT INTO self_referential_setnull(id) VALUES (1)`
		if _, err := db.Exec(run3); err != nil {
			b.Fatal(err)
		}

		for i := 2; i <= numSRRows; i++ {
			insert := fmt.Sprintf(`INSERT INTO self_referential_setnull(id, pid) VALUES (%d, %d)`, i, i-1)
			if _, err := db.Exec(insert); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()
		if _, err := db.Exec(`DELETE FROM self_referential_setnull`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	b.Run("SelfReferential_SetNull_FK_ManyChildren", func(b *testing.B) {
		setup([]string{`self_referential_setnull`})
		defer drop()
		run3 := `INSERT INTO self_referential_setnull(id) VALUES (1)`
		if _, err := db.Exec(run3); err != nil {
			b.Fatal(err)
		}

		for i := 2; i <= numSRRows; i++ {
			insert := fmt.Sprintf(`INSERT INTO self_referential_setnull(id, pid) VALUES (%d, 1)`, i)
			if _, err := db.Exec(insert); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()

		if _, err := db.Exec(`DELETE FROM self_referential_setnull`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})
}
