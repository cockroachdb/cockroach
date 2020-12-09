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

	type testCase struct {
		name      string
		orig, exp func() *tabledesc.Immutable
		ops       func() []ops.Op
	}
	var table *tabledesc.Mutable
	makeTable := func(f func(mutable *tabledesc.Mutable)) func() *tabledesc.Immutable {
		return func() *tabledesc.Immutable {
			cpy := tabledesc.NewExistingMutable(
				*table.ImmutableCopy().(*tabledesc.Immutable).TableDesc())
			if f != nil {
				f(cpy)
			}
			return cpy.ImmutableCopy().(*tabledesc.Immutable)
		}
	}
	run := func(t *testing.T, c testCase) {
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

		require.NoError(t, descs.Txn(ctx, settings, lm, ie, db, func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			ex := executor.New(txn, descriptors)
			orig, err := descriptors.GetTableByName(ctx, txn, &tn, immFlags)
			require.NoError(t, err)
			require.Equal(t, c.orig(), orig)
			require.NoError(t, ex.ExecuteOps(ctx, c.ops()))
			after, err := descriptors.GetTableByName(ctx, txn, &tn, immFlags)
			require.NoError(t, err)
			require.Equal(t, c.exp(), after)
			return nil
		}))
	}

	indexToAdd := descpb.IndexDescriptor{
		ID:          2,
		Name:        "foo",
		ColumnIDs:   []descpb.ColumnID{1},
		ColumnNames: []string{"i"},
		ColumnDirections: []descpb.IndexDescriptor_Direction{
			descpb.IndexDescriptor_ASC,
		},
	}
	for _, tc := range []testCase{
		{
			name: "add index",
			orig: makeTable(nil),
			exp: makeTable(func(mutable *tabledesc.Mutable) {
				mutable.MaybeIncrementVersion()
				mutable.NextIndexID++
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
			ops: func() []ops.Op {
				return []ops.Op{
					ops.AddIndexDescriptor{
						TableID: table.ID,
						Index:   indexToAdd,
					},
				}
			},
		},
		{
			name: "add check constraint",
			orig: makeTable(nil),
			exp: makeTable(func(mutable *tabledesc.Mutable) {
				mutable.MaybeIncrementVersion()
				mutable.Checks = append(mutable.Checks, &descpb.TableDescriptor_CheckConstraint{
					Expr:                "i > 1",
					Name:                "check_foo",
					Validity:            descpb.ConstraintValidity_Validating,
					ColumnIDs:           []descpb.ColumnID{1},
					IsNonNullConstraint: false,
					Hidden:              false,
				})
			}),
			ops: func() []ops.Op {
				return []ops.Op{
					ops.AddCheckConstraint{
						TableID:     table.GetID(),
						Name:        "check_foo",
						Expr:        "i > 1",
						ColumnIDs:   []descpb.ColumnID{1},
						Unvalidated: false,
						Hidden:      false,
					},
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run(t, tc)
		})

	}
}
