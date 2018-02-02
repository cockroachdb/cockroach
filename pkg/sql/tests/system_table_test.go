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

package tests_test

import (
	"context"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestInitialKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const keysPerDesc = 2
	const nonDescKeys = 2

	ms := sqlbase.MakeMetadataSchema()
	kv := ms.GetInitialValues()
	expected := nonDescKeys + keysPerDesc*ms.SystemDescriptorCount()
	if actual := len(kv); actual != expected {
		t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
	}

	// Add an additional table.
	sqlbase.SystemAllowedPrivileges[keys.MaxReservedDescID] = privilege.Lists{{privilege.ALL}}
	desc, err := sql.CreateTestTableDescriptor(
		context.TODO(),
		keys.SystemDatabaseID,
		keys.MaxReservedDescID,
		"CREATE TABLE system.public.x (val INTEGER PRIMARY KEY)",
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
		// Starting with the RoleMembers table, all newly created tables have two sets of privileges:
		// one for "root", another for "admin". Previous tables get them through a migration.
		hasAdmin bool
	}

	for _, test := range []testcase{
		{keys.NamespaceTableID, sqlbase.NamespaceTableSchema, sqlbase.NamespaceTable, false},
		{keys.DescriptorTableID, sqlbase.DescriptorTableSchema, sqlbase.DescriptorTable, false},
		{keys.UsersTableID, sqlbase.UsersTableSchema, sqlbase.UsersTable, false},
		{keys.ZonesTableID, sqlbase.ZonesTableSchema, sqlbase.ZonesTable, false},
		{keys.LeaseTableID, sqlbase.LeaseTableSchema, sqlbase.LeaseTable, false},
		{keys.EventLogTableID, sqlbase.EventLogTableSchema, sqlbase.EventLogTable, false},
		{keys.RangeEventTableID, sqlbase.RangeEventTableSchema, sqlbase.RangeEventTable, false},
		{keys.UITableID, sqlbase.UITableSchema, sqlbase.UITable, false},
		{keys.JobsTableID, sqlbase.JobsTableSchema, sqlbase.JobsTable, false},
		{keys.SettingsTableID, sqlbase.SettingsTableSchema, sqlbase.SettingsTable, false},
		{keys.WebSessionsTableID, sqlbase.WebSessionsTableSchema, sqlbase.WebSessionsTable, false},
		{keys.TableStatisticsTableID, sqlbase.TableStatisticsTableSchema, sqlbase.TableStatisticsTable, false},
		{keys.LocationsTableID, sqlbase.LocationsTableSchema, sqlbase.LocationsTable, false},
		{keys.RoleMembersTableID, sqlbase.RoleMembersTableSchema, sqlbase.RoleMembersTable, true},
	} {
		var privs *sqlbase.PrivilegeDescriptor
		if test.hasAdmin {
			privs = sqlbase.NewCustomSuperuserPrivilegeDescriptor(sqlbase.SystemDesiredPrivileges(test.id))
		} else {
			privs = sqlbase.NewCustomRootPrivilegeDescriptor(sqlbase.SystemDesiredPrivileges(test.id))
		}
		gen, err := sql.CreateTestTableDescriptor(
			context.TODO(),
			keys.SystemDatabaseID,
			test.id,
			test.schema,
			privs,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(&test.pkg, &gen) {
			diff := strings.Join(pretty.Diff(&test.pkg, &gen), "\n")
			t.Errorf("%s table descriptor generated from CREATE TABLE statement does not match "+
				"hardcoded table descriptor:\n%s", test.pkg.Name, diff)
		}
	}
}
