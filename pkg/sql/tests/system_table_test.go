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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestInitialKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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

		ms := bootstrap.MakeMetadataSchema(codec, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
		kv, _ /* splits */ := ms.GetInitialValues()
		expected := nonDescKeys + keysPerDesc*ms.SystemDescriptorCount()
		if actual := len(kv); actual != expected {
			t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
		}

		// Add an additional table.
		descpb.SystemAllowedPrivileges[keys.MaxReservedDescID] = privilege.List{privilege.ALL}
		desc, err := sql.CreateTestTableDescriptor(
			context.Background(),
			keys.SystemDatabaseID,
			keys.MaxReservedDescID,
			"CREATE TABLE system.x (val INTEGER PRIMARY KEY)",
			descpb.NewDefaultPrivilegeDescriptor(security.NodeUserName()),
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
	defer log.Scope(t).Close(t)
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

			ms := bootstrap.MakeMetadataSchema(
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
	defer log.Scope(t).Close(t)
	type testcase struct {
		id     descpb.ID
		schema string
		pkg    catalog.TableDescriptor
	}

	for _, test := range []testcase{
		{keys.NamespaceTableID, systemschema.NamespaceTableSchema, systemschema.NamespaceTable},
		{keys.DescriptorTableID, systemschema.DescriptorTableSchema, systemschema.DescriptorTable},
		{keys.UsersTableID, systemschema.UsersTableSchema, systemschema.UsersTable},
		{keys.ZonesTableID, systemschema.ZonesTableSchema, systemschema.ZonesTable},
		{keys.LeaseTableID, systemschema.LeaseTableSchema, systemschema.LeaseTable},
		{keys.EventLogTableID, systemschema.EventLogTableSchema, systemschema.EventLogTable},
		{keys.RangeEventTableID, systemschema.RangeEventTableSchema, systemschema.RangeEventTable},
		{keys.UITableID, systemschema.UITableSchema, systemschema.UITable},
		{keys.JobsTableID, systemschema.JobsTableSchema, systemschema.JobsTable},
		{keys.SettingsTableID, systemschema.SettingsTableSchema, systemschema.SettingsTable},
		{keys.DescIDSequenceID, systemschema.DescIDSequenceSchema, systemschema.DescIDSequence},
		{keys.TenantsTableID, systemschema.TenantsTableSchema, systemschema.TenantsTable},
		{keys.WebSessionsTableID, systemschema.WebSessionsTableSchema, systemschema.WebSessionsTable},
		{keys.TableStatisticsTableID, systemschema.TableStatisticsTableSchema, systemschema.TableStatisticsTable},
		{keys.LocationsTableID, systemschema.LocationsTableSchema, systemschema.LocationsTable},
		{keys.RoleMembersTableID, systemschema.RoleMembersTableSchema, systemschema.RoleMembersTable},
		{keys.CommentsTableID, systemschema.CommentsTableSchema, systemschema.CommentsTable},
		{keys.ProtectedTimestampsMetaTableID, systemschema.ProtectedTimestampsMetaTableSchema, systemschema.ProtectedTimestampsMetaTable},
		{keys.ProtectedTimestampsRecordsTableID, systemschema.ProtectedTimestampsRecordsTableSchema, systemschema.ProtectedTimestampsRecordsTable},
		{keys.RoleOptionsTableID, systemschema.RoleOptionsTableSchema, systemschema.RoleOptionsTable},
		{keys.StatementBundleChunksTableID, systemschema.StatementBundleChunksTableSchema, systemschema.StatementBundleChunksTable},
		{keys.StatementDiagnosticsRequestsTableID, systemschema.StatementDiagnosticsRequestsTableSchema, systemschema.StatementDiagnosticsRequestsTable},
		{keys.StatementDiagnosticsTableID, systemschema.StatementDiagnosticsTableSchema, systemschema.StatementDiagnosticsTable},
		{keys.ScheduledJobsTableID, systemschema.ScheduledJobsTableSchema, systemschema.ScheduledJobsTable},
		{keys.SqllivenessID, systemschema.SqllivenessTableSchema, systemschema.SqllivenessTable},
		{keys.MigrationsID, systemschema.MigrationsTableSchema, systemschema.MigrationsTable},
		{keys.JoinTokensTableID, systemschema.JoinTokensTableSchema, systemschema.JoinTokensTable},
		{keys.StatementStatisticsTableID, systemschema.StatementStatisticsTableSchema, systemschema.StatementStatisticsTable},
		{keys.TransactionStatisticsTableID, systemschema.TransactionStatisticsTableSchema, systemschema.TransactionStatisticsTable},
	} {
		privs := *test.pkg.GetPrivileges()
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
		require.NoError(t, catalog.ValidateSelf(gen))

		if !test.pkg.TableDesc().Equal(gen.TableDesc()) {
			diff := strings.Join(pretty.Diff(test.pkg.TableDesc(), gen.TableDesc()), "\n")
			t.Errorf("%s table descriptor generated from CREATE TABLE statement does not match "+
				"hardcoded table descriptor:\n%s", test.pkg.GetName(), diff)
		}
	}
}
