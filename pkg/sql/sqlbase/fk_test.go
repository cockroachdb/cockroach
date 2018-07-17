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

package sqlbase

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/kr/pretty"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
)

type testTables struct {
	nextID       ID
	tablesByID   map[ID]*TableDescriptor
	tablesByName map[string]*TableDescriptor
}

func (t *testTables) createTestTable(name string) ID {
	table := &TableDescriptor{
		Name:        name,
		ID:          t.nextID,
		NextIndexID: IndexID(1), // This must be 1 to avoid clashing with a primary index.
	}
	t.tablesByID[table.ID] = table
	t.tablesByName[table.Name] = table
	t.nextID++
	return table.ID
}

func (t *testTables) createForeignKeyReference(
	referencingID ID,
	referencedID ID,
	onDelete ForeignKeyReference_Action,
	onUpdate ForeignKeyReference_Action,
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
	referencedIndex := IndexDescriptor{
		ID: referencedIndexID,
		ReferencedBy: []ForeignKeyReference{
			{
				Table: referencingID,
				Index: referencingIndexID,
			},
		},
	}
	referenced.Indexes = append(referenced.Indexes, referencedIndex)

	referencingIndex := IndexDescriptor{
		ID: referencingIndexID,
		ForeignKey: ForeignKeyReference{
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
		tablesByID:   make(map[ID]*TableDescriptor),
		tablesByName: make(map[string]*TableDescriptor),
	}

	// First setup the table we will be testing against.
	xID := tables.createTestTable("X")

	expectedInsertIDs := []ID{xID}
	expectedUpdateIDs := []ID{xID}
	expectedDeleteIDs := []ID{xID}

	// For all possible combinations of relationships for foreign keys, create a
	// table that X references, and one that references X.
	for deleteNum, deleteName := range ForeignKeyReference_Action_name {
		for updateNum, updateName := range ForeignKeyReference_Action_name {
			subName := fmt.Sprintf("OnDelete%s OnUpdate%s", deleteName, updateName)
			referencedByX := tables.createTestTable(fmt.Sprintf("X Referenced - %s", subName))
			if err := tables.createForeignKeyReference(
				xID, referencedByX, ForeignKeyReference_Action(deleteNum), ForeignKeyReference_Action(updateNum),
			); err != nil {
				t.Fatalf("could not add index: %s", err)
			}

			referencingX := tables.createTestTable(fmt.Sprintf("Referencing X - %s", subName))
			if err := tables.createForeignKeyReference(
				referencingX, xID, ForeignKeyReference_Action(deleteNum), ForeignKeyReference_Action(updateNum),
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
			for deleteNum2, deleteName2 := range ForeignKeyReference_Action_name {
				for updateNum2, updateName2 := range ForeignKeyReference_Action_name {
					//if deleteNum2 != int32(ForeignKeyReference_CASCADE) || updateNum2 != int32(ForeignKeyReference_CASCADE) {
					//	continue
					//}
					subName2 := fmt.Sprintf("Referencing %d - OnDelete%s OnUpdated%s", referencingX, deleteName2, updateName2)
					referencing2 := tables.createTestTable(subName2)
					if err := tables.createForeignKeyReference(
						referencing2, referencingX, ForeignKeyReference_Action(deleteNum2), ForeignKeyReference_Action(updateNum2),
					); err != nil {
						t.Fatalf("could not add index: %s", err)
					}

					// Only fetch the next level of tables if a cascade can occur through
					// the first level.
					if deleteNum == int32(ForeignKeyReference_CASCADE) ||
						deleteNum == int32(ForeignKeyReference_SET_DEFAULT) ||
						deleteNum == int32(ForeignKeyReference_SET_NULL) {
						expectedDeleteIDs = append(expectedDeleteIDs, referencing2)
					}
					if updateNum == int32(ForeignKeyReference_CASCADE) ||
						updateNum == int32(ForeignKeyReference_SET_DEFAULT) ||
						updateNum == int32(ForeignKeyReference_SET_NULL) {
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
			*xDesc,
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
// TODO
func BenchmarkMultiRowFKChecks(b *testing.B) {
	// Throughout the course of testing there are four tables that are set up at the beginning of each sub-benchmark and
	// torn down at the end of each sub-benchmark.
	// `example2` has a foreign key that references `example1`.
	// `self_referential` is a table which has a foreign key reference to itself (parent-child relationship) with
	// 		cascading updates and deletes
	// `self_referential_setnull` has an identical schema to `self_referential` except that instead of cascading
	// 		on delete, it sets the reference field to null.
	schema1 := `
CREATE TABLE IF NOT EXISTS example1(
  foo INT PRIMARY KEY,
  bar INT
)
`
	schema2 := `
CREATE TABLE IF NOT EXISTS example2(
  baz INT PRIMARY KEY,
  foo INT,
  FOREIGN KEY(foo) REFERENCES example1(foo) ON UPDATE CASCADE ON DELETE CASCADE
)
`
	schema3 := `
CREATE TABLE IF NOT EXISTS self_referential(
	id INT PRIMARY KEY,
	pid INT,
	FOREIGN KEY(pid) REFERENCES self_referential(id) ON UPDATE CASCADE ON DELETE CASCADE
)
`
	schema4 := `
CREATE TABLE IF NOT EXISTS self_referential_setnull(
	id INT PRIMARY KEY,
	pid INT,
	FOREIGN KEY(pid) REFERENCES self_referential_setnull(id) ON UPDATE CASCADE ON DELETE SET NULL
)
`
	_, db, _ := serverutils.StartServer(b, base.TestServerArgs{})

	// This function is to be called at the beginning of each sub-benchmark to set up the necessary tables.
	setup := func() {
		setupStatements := []string{schema1, schema2, schema3, schema4}
		for _, s := range setupStatements {
			if _, err := db.Exec(s); err != nil {
				b.Fatal(err)
			}
		}
		run1 := `
INSERT INTO example1(foo) VALUES(1)
`
		if _, err := db.Exec(run1); err != nil {
			b.Fatal(err)
		}
	}

	// This function tears down all the tables and is meant to be called at the end of each sub-benchmark.
	drop := func() {
		tables := [...]string{"example2", "example1", "self_referential", "self_referential_setnull"}
		for _, tableName := range tables {
			if _, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName)); err != nil {
				b.Fatal(err)
			}
		}
	}
	drop()

	// In the following two sub-benchmarks, 10,000 rows are inserted into the `example2` table.
	// All of the rows are inserted in a single statement and deleted in a second statement.
	// In the first test, the rows have no foreign key references to rows in `example1`.
	// In the second, the rows all have references to a particular row in `example1`.
	// The benchmarks primarily measure how insert performance changes when foreign key checks are present.
	const numFKRows = 10000
	b.Run("10KRows_NoFK", func(b *testing.B) {
		setup()
		defer drop()
		b.ResetTimer()
		var run bytes.Buffer
		run.WriteString(`INSERT INTO example2(baz) VALUES `)

		for i := 1; i <= numFKRows; i++ {
			run.WriteString(fmt.Sprintf("(%d)", i))
			if i != numFKRows {
				run.WriteString(", ")
			}
		}

		b.StopTimer()
		if _, err := db.Exec(`DELETE FROM example2`); err != nil {
			b.Fatal(err)
		}
	})
	b.Run("10KRows_IdenticalFK", func(b *testing.B) {
		setup()
		defer drop()
		b.ResetTimer()
		var run bytes.Buffer

		run.WriteString(`INSERT INTO example2(baz, foo) VALUES `)

		for i := 1; i <= numFKRows; i++ {
			run.WriteString("(")
			run.WriteString(strconv.Itoa(i))
			run.WriteString(", 1")
			if i != numFKRows {
				run.WriteString("), ")
			} else {
				run.WriteString(")")
			}
		}

		statement := run.String()
		if _, err := db.Exec(statement); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if _, err := db.Exec(`DELETE FROM example2`); err != nil {
			b.Fatal(err)
		}
	})

	// For the self-referential table benchmarks, 10,000 rows are inserted and there is again a contrast between
	// rows with foreign key references and those without.
	// This measures the performance of cascading deletes.
	const numSRRows = 10000
	b.Run("SelfReferential_No_FK_Delete", func(b *testing.B) {
		setup()
		defer drop()
		var insert bytes.Buffer
		insert.WriteString(`INSERT INTO self_referential(id) VALUES `)
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

		if _, err := db.Exec(`DELETE FROM self_referential`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})

	b.Run("SelfReferential_FK_Delete", func(b *testing.B) {
		setup()
		defer drop()
		run3 := `INSERT INTO self_referential(id) VALUES (1)`
		if _, err := db.Exec(run3); err != nil {
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

	// The SelfReferential_SetNull tests perform the same delete tests as the SelfReferential tests, but because of the
	// differing schemas, the deletes do not cascade.
	b.Run("SelfReferential_SetNull_NoFK_Delete", func(b *testing.B) {
		setup()
		defer drop()
		var insert bytes.Buffer
		insert.WriteString(`INSERT INTO self_referential_setnull(id) VALUES `)
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

		if _, err := db.Exec(`DELETE FROM self_referential_setnull`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	})
	b.Run("SelfReferential_SetNull_FK_Delete", func(b *testing.B) {
		setup()
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
}
