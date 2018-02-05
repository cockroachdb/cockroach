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
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/kr/pretty"
	"github.com/pkg/errors"
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
