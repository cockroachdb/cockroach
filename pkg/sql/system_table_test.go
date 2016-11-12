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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

func TestInitialKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const nonSystemDesc = 4
	const keysPerDesc = 2
	const nonDescKeys = 2

	ms := sqlbase.MakeMetadataSchema()
	kv := ms.GetInitialValues()
	expected := nonDescKeys + keysPerDesc*(nonSystemDesc+ms.SystemConfigDescriptorCount())
	if actual := len(kv); actual != expected {
		t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
	}

	// Add an additional table.
	desc, err := sql.CreateTestTableDescriptor(
		keys.SystemDatabaseID,
		keys.MaxSystemConfigDescID+1,
		"CREATE TABLE testdb.x (val INTEGER PRIMARY KEY)",
		sqlbase.NewDefaultPrivilegeDescriptor(),
	)
	if err != nil {
		t.Fatal(err)
	}
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
		gen, err := sql.CreateTestTableDescriptor(
			keys.SystemDatabaseID,
			test.id,
			test.schema,
			sqlbase.NewPrivilegeDescriptor(security.RootUser, sqlbase.SystemConfigAllowedPrivileges[test.id]),
		)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(&test.pkg, &gen) {
			t.Fatalf(
				"mismatch between re-generated version and pkg version of %s:\npkg:\n\t%#v\ngenerated\n\t%#v",
				test.pkg.Name, test.pkg, gen)
		}
	}
	// test the tables with non-specific NewDefaultPrivilegeDescriptor
	for _, test := range []testcase{
		{keys.LeaseTableID, sqlbase.LeaseTableSchema, sqlbase.LeaseTable},
		{keys.EventLogTableID, sqlbase.EventLogTableSchema, sqlbase.EventLogTable},
		{keys.RangeEventTableID, sqlbase.RangeEventTableSchema, sqlbase.RangeEventTable},
		{keys.UITableID, sqlbase.UITableSchema, sqlbase.UITable},
	} {
		gen, err := sql.CreateTestTableDescriptor(
			keys.SystemDatabaseID, test.id, test.schema, sqlbase.NewDefaultPrivilegeDescriptor(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(&test.pkg, &gen) {
			s1 := fmt.Sprintf("%#v", test.pkg)
			s2 := fmt.Sprintf("%#v", gen)
			for i := range s1 {
				if s1[i] != s2[i] {
					t.Fatalf(
						"mismatch between %s:\npkg:\n\t%#v\npartial-gen\n\t%#v\ngen\n\t%#v",
						test.pkg.Name, s1[:i+3], s2[:i+3], gen)
				}
			}
			panic("did not locate mismatch between re-generated version and pkg version")
		}
	}
}
