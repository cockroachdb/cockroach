package executor_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/executor"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestExecutorDescriptorMutationOps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `
CREATE TABLE db.t (
   i INT PRIMARY KEY 
)`)

	settings := tc.Server(0).ClusterSettings()
	ie := tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor)
	db := tc.Server(0).DB()
	lm := tc.Server(0).LeaseManager().(*lease.Manager)
	var table *tabledesc.Mutable
	mutFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			Required:       true,
			RequireMutable: true,
			AvoidCached:    true,
		},
	}
	immFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			Required:    true,
			AvoidCached: true,
		},
	}
	tn := tree.MakeTableName("db", "t")
	require.NoError(t, descs.Txn(ctx, settings, lm, ie, db, func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {

		td, err := descriptors.GetTableByName(ctx, txn, &tn, mutFlags)
		if err != nil {
			return err
		}
		table = td.(*tabledesc.Mutable)
		return nil
	}))
	makeTable := func(f func(mutable *tabledesc.Mutable)) *tabledesc.Immutable {
		cpy := tabledesc.NewExistingMutable(
			*table.ImmutableCopy().(*tabledesc.Immutable).TableDesc())
		if f != nil {
			f(cpy)
		}
		return cpy.ImmutableCopy().(*tabledesc.Immutable)
	}
	indexToAdd := descpb.IndexDescriptor{
		ID:        2,
		Name:      "foo",
		ColumnIDs: []descpb.ColumnID{1},
	}
	for _, tc := range []struct {
		name      string
		orig, exp *tabledesc.Immutable
		ops       []ops.Op
	}{
		{
			name: "add index",
			orig: makeTable(nil),
			exp: makeTable(func(mutable *tabledesc.Mutable) {
				mutable.MaybeIncrementVersion()
				mutable.Mutations = append(mutable.Mutations, descpb.DescriptorMutation{
					Descriptor_: &descpb.DescriptorMutation_Index{
						Index: &indexToAdd,
					},
					State:      descpb.DescriptorMutation_DELETE_ONLY,
					Direction:  descpb.DescriptorMutation_ADD,
					MutationID: mutable.NextMutationID,
				})
				mutable.NextMutationID++
			}),
			ops: []ops.Op{
				ops.AddIndexDescriptor{
					TableID: table.ID,
					Index:   indexToAdd,
				},
			},
		},
		{
			name: "add check constraint",
			orig: makeTable(nil),
			exp: makeTable(func(mutable *tabledesc.Mutable) {

			}),
		},
	} {

		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, descs.Txn(ctx, settings, lm, ie, db, func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
				ex := executor.New(txn, descriptors)
				orig, err := descriptors.GetTableByName(ctx, txn, &tn, immFlags)
				require.NoError(t, err)
				require.Equal(t, tc.orig, orig)
				require.NoError(t, ex.ExecuteOps(ctx, tc.ops))
				after, err := descriptors.GetTableByName(ctx, txn, &tn, immFlags)
				require.NoError(t, err)
				require.Equal(t, tc.exp, after)
				return nil
			}))
		})

	}
}
