// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
)

func TestInitialKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const keysPerDesc = 2
	const nonDescKeys = 9

	ms := sqlbase.MakeMetadataSchema(zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
	kv, _ /* splits */ := ms.GetInitialValues()
	expected := nonDescKeys + keysPerDesc*ms.SystemDescriptorCount()
	if actual := len(kv); actual != expected {
		t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
	}

	// Add an additional table.
	sqlbase.SystemAllowedPrivileges[keys.MaxReservedDescID] = privilege.List{privilege.ALL}
	desc, err := sql.CreateTestTableDescriptor(
		context.TODO(),
		keys.SystemDatabaseID,
		keys.MaxReservedDescID,
		"CREATE TABLE system.x (val INTEGER PRIMARY KEY)",
		sqlbase.NewDefaultPrivilegeDescriptor(),
	)
	if err != nil {
		t.Fatal(err)
	}
	ms.AddDescriptor(keys.SystemDatabaseID, &desc)
	kv, _ /* splits */ = ms.GetInitialValues()
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
	if a, e := i, int64(keys.MinUserDescID); a != e {
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

	for _, test := range []testcase{
		{keys.NamespaceTableID, sqlbase.NamespaceTableSchema, sqlbase.NamespaceTable},
		{keys.DescriptorTableID, sqlbase.DescriptorTableSchema, sqlbase.DescriptorTable},
		{keys.UsersTableID, sqlbase.UsersTableSchema, sqlbase.UsersTable},
		{keys.ZonesTableID, sqlbase.ZonesTableSchema, sqlbase.ZonesTable},
		{keys.LeaseTableID, sqlbase.LeaseTableSchema, sqlbase.LeaseTable},
		{keys.EventLogTableID, sqlbase.EventLogTableSchema, sqlbase.EventLogTable},
		{keys.RangeEventTableID, sqlbase.RangeEventTableSchema, sqlbase.RangeEventTable},
		{keys.UITableID, sqlbase.UITableSchema, sqlbase.UITable},
		{keys.JobsTableID, sqlbase.JobsTableSchema, sqlbase.JobsTable},
		{keys.SettingsTableID, sqlbase.SettingsTableSchema, sqlbase.SettingsTable},
		{keys.WebSessionsTableID, sqlbase.WebSessionsTableSchema, sqlbase.WebSessionsTable},
		{keys.TableStatisticsTableID, sqlbase.TableStatisticsTableSchema, sqlbase.TableStatisticsTable},
		{keys.LocationsTableID, sqlbase.LocationsTableSchema, sqlbase.LocationsTable},
		{keys.RoleMembersTableID, sqlbase.RoleMembersTableSchema, sqlbase.RoleMembersTable},
		{keys.CommentsTableID, sqlbase.CommentsTableSchema, sqlbase.CommentsTable},
	} {
		privs := *test.pkg.Privileges
		gen, err := sql.CreateTestTableDescriptor(
			context.TODO(),
			keys.SystemDatabaseID,
			test.id,
			test.schema,
			&privs,
		)
		if err != nil {
			t.Fatalf("test: %+v, err: %v", test, err)
		}

		if !proto.Equal(&test.pkg, &gen) {
			diff := strings.Join(pretty.Diff(&test.pkg, &gen), "\n")
			t.Errorf("%s table descriptor generated from CREATE TABLE statement does not match "+
				"hardcoded table descriptor:\n%s", test.pkg.Name, diff)
		}
	}
}
