// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestRenameTable tests the table descriptor changes during
// a rename operation.
func TestRenameTable(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, sqlDB, kvDB := setup(t)
	defer cleanup(s, sqlDB)

	// The first `MaxReservedDescID` (as well as 0) are set aside.
	counter := int64(sql.MaxReservedDescID + 1)

	// Table creation should fail, and nothing should have been written.
	oldDBID := sql.ID(counter)
	if _, err := sqlDB.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}
	counter++

	// Create table in 'test'.
	tableCounter := counter
	oldName := "foo"
	if _, err := sqlDB.Exec(`CREATE TABLE test.foo (k INT PRIMARY KEY, v int)`); err != nil {
		t.Fatal(err)
	}
	counter++

	// Check the table descriptor.
	var descriptor sql.TableDescriptor
	tableDescKey := sql.MakeDescMetadataKey(sql.ID(tableCounter))
	if err := kvDB.GetProto(tableDescKey, &descriptor); err != nil {
		t.Fatal(err)
	}
	if descriptor.Name != oldName {
		t.Fatalf("Wrong table name, expected %s, got: %+v", oldName, descriptor)
	}
	if descriptor.ParentID != oldDBID {
		t.Fatalf("Wrong parent ID on table, expected %d, got: %+v", oldDBID, descriptor)
	}

	// Create database test2.
	newDBID := sql.ID(counter)
	if _, err := sqlDB.Exec(`CREATE DATABASE test2`); err != nil {
		t.Fatal(err)
	}
	counter++

	// Move table to test2 and change its name as well.
	newName := "bar"
	if _, err := sqlDB.Exec(`ALTER TABLE test.foo RENAME TO test2.bar`); err != nil {
		t.Fatal(err)
	}

	// Check the table descriptor again.
	if err := kvDB.GetProto(tableDescKey, &descriptor); err != nil {
		t.Fatal(err)
	}
	if descriptor.Name != newName {
		t.Fatalf("Wrong table name, expected %s, got: %+v", newName, descriptor)
	}
	if descriptor.ParentID != newDBID {
		t.Fatalf("Wrong parent ID on table, expected %d, got: %+v", newDBID, descriptor)
	}

}
