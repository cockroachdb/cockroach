// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestInterleavedTableMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.InterleavedCreationBlockedMigration - 1),
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(db)
	tdb.ExecSucceedsSoon(t, "CREATE TABLE customers (id INT PRIMARY KEY, name STRING(50));")
	tdb.ExecSucceedsSoon(t, `CREATE TABLE orders (
   customer INT,
   id INT,
   total DECIMAL(20, 5),
   PRIMARY KEY (customer, id),
   CONSTRAINT fk_customer FOREIGN KEY (customer) REFERENCES customers
 ) INTERLEAVE IN PARENT customers (customer);`)

	// Migration should have failed.
	tdb.ExpectErr(t, "pq: running migration for 21.1-120: interleaved tables are no longer supported", "SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.InterleavedCreationBlockedMigration).String())
	// Save the old descriptors.
	server := tc.Server(0)
	var customersDescClone []byte = nil
	var ordersDescClone []byte = nil
	require.NoError(t, descs.Txn(ctx, server.ClusterSettings(), server.LeaseManager().(*lease.Manager), server.InternalExecutor().(sqlutil.InternalExecutor), server.DB(),
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			_, customersDesc, err := descriptors.GetMutableTableByName(ctx, txn, tree.NewQualifiedObjectName("defaultdb", "public", "customers", tree.TableObject), tree.ObjectLookupFlagsWithRequired())
			if err != nil {
				return errors.Wrap(err, "Failed fetching customers descriptor")
			}
			customersDescClone, err = protoutil.Marshal(customersDesc)
			if err != nil {
				return errors.Wrap(err, "Failed marshaling customers descriptor")
			}
			_, ordersDesc, err := descriptors.GetMutableTableByName(ctx, txn, tree.NewQualifiedObjectName("defaultdb", "public", "orders", tree.TableObject), tree.ObjectLookupFlagsWithRequired())
			if err != nil {
				return errors.Wrap(err, "Failed fetching orders descriptor")
			}
			ordersDescClone, err = protoutil.Marshal(ordersDesc)
			if err != nil {
				return errors.Wrap(err, "Failed marshaling orders descriptor")
			}
			return nil
		}))
	// Next drop the old descriptor and wait for the jobs to complete.
	_, err := db.Exec(`ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds=1;`)
	require.NoError(t, err)
	tdb.ExecSucceedsSoon(t, `DROP TABLE orders;`)
	tdb.ExecSucceedsSoon(t, `DROP TABLE customers;`)
	testutils.SucceedsSoon(t, func() error {
		row := tdb.QueryRow(t,
			`SELECT count(*) from [show jobs]  where status not in ('succeeded', 'failed', 'aborted')`)
		count := 0
		row.Scan(&count)
		if count != 0 {
			return errors.New("Waiting for GC jobs to complete")
		}
		return nil
	})

	// Migration should succeed.
	tdb.ExecSucceedsSoon(t, "SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.InterleavedCreationBlockedMigration).String())
	// Inject the old descriptors next phase should fail.
	var customerDesc tabledesc.Mutable
	var ordersDesc tabledesc.Mutable
	require.NoError(t, protoutil.Unmarshal(customersDescClone, &customerDesc))
	require.NoError(t, protoutil.Unmarshal(ordersDescClone, &ordersDesc))
	require.NoError(t, sqlutils.InjectDescriptors(ctx, db, []*descpb.Descriptor{customerDesc.DescriptorProto(), ordersDesc.DescriptorProto()}, true))
	tdb.ExpectErr(t, "pq: running migration for 21.1-122: interleaved tables are no longer supported at this version, please drop or uninterleave them", "SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.InterleavedTablesRemovedMigration).String())
	// Next drop the cloneed descriptors
	tdb.ExecSucceedsSoon(t, `DROP TABLE orders;`)
	tdb.ExecSucceedsSoon(t, `DROP TABLE customers;`)
	// Migration should succeed.
	tdb.ExecSucceedsSoon(t, "SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.InterleavedTablesRemovedMigration).String())
}
