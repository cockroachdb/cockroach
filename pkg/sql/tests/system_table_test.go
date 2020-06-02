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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestInitialKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const keysPerDesc = 2

	testutils.RunTrueAndFalse(t, "system tenant", func(t *testing.T, systemTenant bool) {
		var codec keys.SQLCodec
		var nonDescKeys int
		if systemTenant {
			codec = keys.SystemSQLCodec
			nonDescKeys = 9
		} else {
			codec = keys.MakeSQLCodec(roachpb.MakeTenantID(5))
			nonDescKeys = 2
		}

		ms := sqlbase.MakeMetadataSchema(codec, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
		kv, _ /* splits */ := ms.GetInitialValues()
		expected := nonDescKeys + keysPerDesc*ms.SystemDescriptorCount()
		if actual := len(kv); actual != expected {
			t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
		}

		// Add an additional table.
		sqlbase.SystemAllowedPrivileges[keys.MaxReservedDescID] = privilege.List{privilege.ALL}
		desc, err := sql.CreateTestTableDescriptor(
			context.Background(),
			keys.SystemDatabaseID,
			keys.MaxReservedDescID,
			"CREATE TABLE system.x (val INTEGER PRIMARY KEY)",
			sqlbase.NewDefaultPrivilegeDescriptor(),
		)
		if err != nil {
			t.Fatal(err)
		}
		ms.AddDescriptor(keys.SystemDatabaseID, desc)
		kv, _ /* splits */ = ms.GetInitialValues()
		expected = nonDescKeys + keysPerDesc*ms.SystemDescriptorCount()
		if actual := len(kv); actual != expected {
			t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
		}

		// Verify that IDGenerator value is correct.
		found := false
		idgen := codec.DescIDSequenceKey()
		var idgenkv roachpb.KeyValue
		for _, v := range kv {
			if v.Key.Equal(idgen) {
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
	})
}

func TestInitialKeysAndSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.RunTest(t, "testdata/initial_keys", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "initial-keys":
			var tenant string
			d.ScanArgs(t, "tenant", &tenant)

			var codec keys.SQLCodec
			if tenant == "system" {
				codec = keys.SystemSQLCodec
			} else {
				id, err := strconv.ParseUint(tenant, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				codec = keys.MakeSQLCodec(roachpb.MakeTenantID(id))
			}

			ms := sqlbase.MakeMetadataSchema(
				codec, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef(),
			)
			kvs, splits := ms.GetInitialValues()

			var buf strings.Builder
			fmt.Fprintf(&buf, "%d keys:\n", len(kvs))
			for _, kv := range kvs {
				fmt.Fprintf(&buf, " %s\n", kv.Key)
			}
			fmt.Fprintf(&buf, "%d splits:\n", len(splits))
			for _, k := range splits {
				fmt.Fprintf(&buf, " %s\n", k.AsRawKey())
			}
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
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
		pkg    *sqlbase.ImmutableTableDescriptor
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
		{keys.DescIDSequenceID, sqlbase.DescIDSequenceSchema, sqlbase.DescIDSequence},
		{keys.TenantsTableID, sqlbase.TenantsTableSchema, sqlbase.TenantsTable},
		{keys.WebSessionsTableID, sqlbase.WebSessionsTableSchema, sqlbase.WebSessionsTable},
		{keys.TableStatisticsTableID, sqlbase.TableStatisticsTableSchema, sqlbase.TableStatisticsTable},
		{keys.LocationsTableID, sqlbase.LocationsTableSchema, sqlbase.LocationsTable},
		{keys.RoleMembersTableID, sqlbase.RoleMembersTableSchema, sqlbase.RoleMembersTable},
		{keys.CommentsTableID, sqlbase.CommentsTableSchema, sqlbase.CommentsTable},
		{keys.ProtectedTimestampsMetaTableID, sqlbase.ProtectedTimestampsMetaTableSchema, sqlbase.ProtectedTimestampsMetaTable},
		{keys.ProtectedTimestampsRecordsTableID, sqlbase.ProtectedTimestampsRecordsTableSchema, sqlbase.ProtectedTimestampsRecordsTable},
		{keys.RoleOptionsTableID, sqlbase.RoleOptionsTableSchema, sqlbase.RoleOptionsTable},
		{keys.StatementBundleChunksTableID, sqlbase.StatementBundleChunksTableSchema, sqlbase.StatementBundleChunksTable},
		{keys.StatementDiagnosticsRequestsTableID, sqlbase.StatementDiagnosticsRequestsTableSchema, sqlbase.StatementDiagnosticsRequestsTable},
		{keys.StatementDiagnosticsTableID, sqlbase.StatementDiagnosticsTableSchema, sqlbase.StatementDiagnosticsTable},
		{keys.ScheduledJobsTableID, sqlbase.ScheduledJobsTableSchema, sqlbase.ScheduledJobsTable},
	} {
		privs := *test.pkg.Privileges
		gen, err := sql.CreateTestTableDescriptor(
			context.Background(),
			keys.SystemDatabaseID,
			test.id,
			test.schema,
			&privs,
		)
		if err != nil {
			t.Fatalf("test: %+v, err: %v", test, err)
		}
		require.NoError(t, gen.ValidateTable())

		if !proto.Equal(test.pkg.TableDesc(), gen.TableDesc()) {
			diff := strings.Join(pretty.Diff(&test.pkg, &gen), "\n")
			t.Errorf("%s table descriptor generated from CREATE TABLE statement does not match "+
				"hardcoded table descriptor:\n%s", test.pkg.Name, diff)
		}
	}
}
