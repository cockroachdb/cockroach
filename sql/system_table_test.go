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
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

func TestInitialKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const nonSystemDesc = 2
	const keysPerDesc = 2
	const nonDescKeys = 2

	ms := sqlbase.MakeMetadataSchema()
	kv := ms.GetInitialValues()
	expected := nonDescKeys + keysPerDesc*(nonSystemDesc+ms.SystemConfigDescriptorCount())
	if actual := len(kv); actual != expected {
		t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
	}

	// Add an additional table.
	desc := sql.CreateTableDescriptor(keys.MaxSystemConfigDescID+1, keys.SystemDatabaseID, "CREATE TABLE testdb.x (val INTEGER PRIMARY KEY)", sqlbase.NewDefaultPrivilegeDescriptor())
	ms.AddDescriptor(keys.SystemDatabaseID, &desc)
	kv = ms.GetInitialValues()
	expected = nonDescKeys + keysPerDesc*ms.SystemDescriptorCount()
	if actual := len(kv); actual != expected {
		t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
	}

	// Verify that IDGenerator value is correct.
	found := false
	var idgenkv roachpb.KeyValue
	for _, v := range kv {
		if v.Key.Equal(keys.DescIDGenerator) {
			idgenkv = v
			found = true
			break
		}
	}

	if !found {
		t.Fatal("Could not find descriptor ID generator in initial key set")
	}
	// Expect 2 non-reserved IDs to have been allocated.
	i, err := idgenkv.Value.GetInt()
	if err != nil {
		t.Fatal(err)
	}
	if a, e := i, int64(keys.MaxReservedDescID+1); a != e {
		t.Fatalf("Expected next descriptor ID to be %d, was %d", e, a)
	}
}

// TestSystemTableLiterals compares the result of evaluating the `CREATE TABLE`
// statement strings that describe each system table with the TableDescriptor
// literals that are actually used at runtime. This ensures we can use the hand-
// written literals instead of having to evaluate the `CREATE TABLE` statements
// before initialization and with limited SQL machinery bootstraped, while still
// confident that the result is the same as if `CREATE TABLE` had been run.
//
// This test may also be useful when writing a new system table:
// adding the new schema along with a trivial, empty TableDescriptor literal
// will print the expected proto which can then be used to replace the empty
// one (though pruning the explicit zero values may make it more readable).
func TestSystemTableLiterals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testcase struct {
		id     sqlbase.ID
		schema string
		pkg    sqlbase.TableDescriptor
	}

	// test the tables with specific permissions
	for _, test := range []testcase{
		{keys.NamespaceTableID, sqlbase.NamespaceTableSchema, sqlbase.NamespaceTable},
		{keys.DescriptorTableID, sqlbase.DescriptorTableSchema, sqlbase.DescriptorTable},
		{keys.UsersTableID, sqlbase.UsersTableSchema, sqlbase.UsersTable},
		{keys.ZonesTableID, sqlbase.ZonesTableSchema, sqlbase.ZonesTable},
	} {
		gen := sql.CreateTableDescriptor(test.id, keys.SystemDatabaseID, test.schema,
			sqlbase.NewPrivilegeDescriptor(security.RootUser, sqlbase.SystemConfigAllowedPrivileges[test.id]))
		if !proto.Equal(&test.pkg, &gen) {
			t.Fatalf(
				"mismatch between re-generated version and pkg version of %s:\npkg:\n\t%#v\ngenerated\n\t%#v",
				test.pkg.Name, test.pkg, gen)
		}
	}
	// test the tables with non-specific NewDefaultPrivilegeDescriptor
	for _, test := range []testcase{
		{keys.LeaseTableID, sqlbase.LeaseTableSchema, sqlbase.LeaseTable},
		{keys.UITableID, sqlbase.UITableSchema, sqlbase.UITable},
	} {
		gen := sql.CreateTableDescriptor(test.id, keys.SystemDatabaseID, test.schema, sqlbase.NewDefaultPrivilegeDescriptor())
		if !proto.Equal(&test.pkg, &gen) {
			t.Fatalf(
				"mismatch between re-generated version and pkg version of %s:\npkg:\n\t%#v\ngenerated\n\t%#v",
				test.pkg.Name, test.pkg, gen)
		}
	}
}
