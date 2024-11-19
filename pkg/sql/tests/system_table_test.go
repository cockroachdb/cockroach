// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
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
			nonDescKeys = 17
		} else {
			codec = keys.MakeSQLCodec(roachpb.MustMakeTenantID(5))
			nonDescKeys = 8
		}

		ms := bootstrap.MakeMetadataSchema(codec, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
		kv, _ /* splits */ := ms.GetInitialValues()
		expected := nonDescKeys + keysPerDesc*ms.SystemDescriptorCount()
		if actual := len(kv); actual != expected {
			t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
		}

		// Add an additional table.
		desc, err := sql.CreateTestTableDescriptor(
			context.Background(),
			keys.SystemDatabaseID,
			descpb.ID(1000 /* suitably large descriptor ID */),
			"CREATE TABLE system.x (val INTEGER PRIMARY KEY)",
			catpb.NewBasePrivilegeDescriptor(username.NodeUserName()),
			nil,
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		ms.AddDescriptor(desc)
		kv, _ /* splits */ = ms.GetInitialValues()
		expected = nonDescKeys + keysPerDesc*ms.SystemDescriptorCount()
		if actual := len(kv); actual != expected {
			t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
		}

		// Verify that IDGenerator value is correct.
		found := false
		idgen := codec.SequenceKey(keys.DescIDSequenceID)
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
		if a, e := i, int64(desc.GetID()+1); a != e {
			t.Fatalf("Expected next descriptor ID to be %d, was %d", e, a)
		}
	})
}

func TestInitialKeysAndSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "initial_keys"), func(t *testing.T, d *datadriven.TestData) string {
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
				codec = keys.MakeSQLCodec(roachpb.MustMakeTenantID(id))
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
// before initialization and with limited SQL machinery bootstrapped, while
// still confident that the result is the same as if `CREATE TABLE` had been
// run.
//
// This test may also be useful when writing a new system table:
// adding the new schema along with a trivial, empty TableDescriptor literal
// will print the expected proto which can then be used to replace the empty
// one (though pruning the explicit zero values may make it more readable).
func TestSystemTableLiterals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testcase struct {
		schema string
		pkg    catalog.TableDescriptor
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)

	s := tc.Servers[0]

	testcases := make(map[string]testcase)
	for _, table := range systemschema.MakeSystemTables() {
		if _, alreadyExists := testcases[table.GetName()]; alreadyExists {
			t.Fatalf("system table %q already exists", table.GetName())
		}
		testcases[table.GetName()] = testcase{
			schema: table.Schema,
			pkg:    table,
		}
	}

	require.Equal(t, bootstrap.NumSystemTablesForSystemTenant, len(testcases))

	runTest := func(t *testing.T, name string, test testcase) {
		privs := *test.pkg.GetPrivileges()
		desc := test.pkg
		// Allocate an ID to dynamically allocated system tables.
		if desc.GetID() == 0 {
			mut := desc.NewBuilder().BuildCreatedMutable().(*tabledesc.Mutable)
			mut.ID = keys.MaxReservedDescID + 1
			desc = mut.ImmutableCopy().(catalog.TableDescriptor)
		}
		leaseManager := s.LeaseManager().(*lease.Manager)
		collection := descs.MakeTestCollection(ctx, keys.SystemSQLCodec, leaseManager)

		gen, err := sql.CreateTestTableDescriptor(
			context.Background(),
			keys.SystemDatabaseID,
			desc.GetID(),
			test.schema,
			&privs,
			s.DB().NewTxn(ctx, "create-test-table-desc"),
			&collection,
		)
		if err != nil {
			t.Fatalf("test: %+v, err: %v", test, err)
		}
		require.NoError(t, desctestutils.TestingValidateSelf(gen))

		// The tables with regional by row compatible indexes had their
		// indexes rewritten to ID 2. There is no way to specify index
		// ids in SQL, so we need to manually patch the descriptor to
		// get the sql constructed descriptor to match the statically
		// constructed descriptor.
		switch gen.GetID() {
		case keys.SqllivenessID:
			gen.TableDescriptor.PrimaryIndex.ID = 2
			gen.TableDescriptor.NextIndexID = 3
		case keys.SQLInstancesTableID:
			gen.TableDescriptor.PrimaryIndex.ID = 2
			gen.TableDescriptor.NextIndexID = 3
		case keys.LeaseTableID:
			gen.TableDescriptor.PrimaryIndex.ID = 3
			gen.TableDescriptor.NextIndexID = 4
		}

		if desc.TableDesc().Equal(gen.TableDesc()) {
			return
		}
		diff := strings.Join(pretty.Diff(desc.TableDesc(), gen.TableDesc()), "\n")
		t.Errorf("%s table descriptor generated from CREATE TABLE statement does not match "+
			"hardcoded table descriptor:\n%s", desc.GetName(), diff)
	}

	for name, test := range testcases {
		t.Run(name, func(t *testing.T) {
			runTest(t, name, test)
		})
	}
}
