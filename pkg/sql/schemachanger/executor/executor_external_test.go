package executor_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/compiler"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/executor"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/stretchr/testify/require"
)

type testInfra struct {
	tc       *testcluster.TestCluster
	settings *cluster.Settings
	ie       sqlutil.InternalExecutor
	db       *kv.DB
	lm       *lease.Manager
	tsql     *sqlutils.SQLRunner
}

func setupTestInfra(t testing.TB) *testInfra {
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	return &testInfra{
		tc:       tc,
		settings: tc.Server(0).ClusterSettings(),
		ie:       tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor),
		db:       tc.Server(0).DB(),
		lm:       tc.Server(0).LeaseManager().(*lease.Manager),
		tsql:     sqlutils.MakeSQLRunner(tc.ServerConn(0)),
	}
}

func (ti *testInfra) txn(
	ctx context.Context,
	f func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error,
) error {
	return descs.Txn(ctx, ti.settings, ti.lm, ti.ie, ti.db, f)
}

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
	run := func(t *testing.T, c testCase) {
		ctx := context.Background()
		ti := setupTestInfra(t)
		defer ti.tc.Stopper().Stop(ctx)

		ti.tsql.Exec(t, `CREATE DATABASE db`)
		ti.tsql.Exec(t, `
CREATE TABLE db.t (
   i INT PRIMARY KEY 
)`)

		tn := tree.MakeTableName("db", "t")
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			td, err := descriptors.GetTableByName(ctx, txn, &tn, mutFlags)
			if err != nil {
				return err
			}
			table = td.(*tabledesc.Mutable)
			return nil
		}))

		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
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

// TODO(ajwerner): Move this out into the schemachanger_test package once that
// is fixed up.
func TestSchemaChanger(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	t.Run("add column", func(t *testing.T) {
		ti := setupTestInfra(t)
		defer ti.tc.Stopper().Stop(ctx)
		ti.tsql.Exec(t, `CREATE DATABASE db`)
		ti.tsql.Exec(t, `CREATE TABLE db.foo (i INT PRIMARY KEY)`)

		var id descpb.ID
		var ts []targets.TargetState
		var targetSlice []targets.Target
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) (err error) {
			tn := tree.MakeTableName("db", "foo")
			fooTable, err := descriptors.GetTableByName(ctx, txn, &tn, tree.ObjectLookupFlagsWithRequired())
			require.NoError(t, err)
			id = fooTable.GetID()

			// Corresponds to:
			//
			//  ALTER TABLE foo ADD COLUMN j INT;
			//
			targetSlice = []targets.Target{
				&targets.AddIndex{
					TableID: fooTable.GetID(),
					Index: descpb.IndexDescriptor{
						Name:             "primary 2",
						ID:               2,
						ColumnIDs:        []descpb.ColumnID{1},
						ColumnNames:      []string{"i"},
						ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						StoreColumnIDs:   []descpb.ColumnID{2},
						StoreColumnNames: []string{"j"},
						Unique:           true,
						Type:             descpb.IndexDescriptor_FORWARD,
					},
					PrimaryIndex:   fooTable.GetPrimaryIndexID(),
					ReplacementFor: fooTable.GetPrimaryIndexID(),
					Primary:        true,
				},
				&targets.AddColumn{
					TableID:      fooTable.GetID(),
					ColumnFamily: descpb.FamilyID(1),
					Column: descpb.ColumnDescriptor{
						Name:           "j",
						ID:             2,
						Type:           types.Int,
						Nullable:       true,
						PGAttributeNum: 2,
					},
				},
				&targets.DropIndex{
					TableID:    fooTable.GetID(),
					IndexID:    fooTable.GetPrimaryIndexID(),
					ReplacedBy: 2,
					ColumnIDs:  []descpb.ColumnID{1},
				},
			}

			targetStates := []targets.TargetState{
				{
					Target: targetSlice[0],
					State:  targets.StateAbsent,
				},
				{
					Target: targetSlice[1],
					State:  targets.StateAbsent,
				},
				{
					Target: targetSlice[2],
					State:  targets.StatePublic,
				},
			}

			for _, phase := range []compiler.ExecutionPhase{
				compiler.PostStatementPhase,
				compiler.PreCommitPhase,
			} {
				stages, err := compiler.Compile(targetStates, compiler.CompileFlags{
					ExecutionPhase: phase,
				})
				require.NoError(t, err)
				for _, s := range stages {
					require.NoError(t, executor.New(txn, descriptors).ExecuteOps(ctx, s.Ops))
					ts = s.NextTargets
				}
			}
			return nil
		}))
		var after []targets.TargetState
		ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			stages, err := compiler.Compile(ts, compiler.CompileFlags{
				ExecutionPhase: compiler.PostCommitPhase,
			})
			require.NoError(t, err)
			for _, s := range stages {
				require.NoError(t, executor.New(txn, descriptors).ExecuteOps(ctx, s.Ops))
				after = s.NextTargets
			}
			return nil
		})
		require.Equal(t, []targets.TargetState{
			{
				targetSlice[0],
				targets.StatePublic,
			},
			{
				targetSlice[1],
				targets.StatePublic,
			},
			{
				targetSlice[2],
				targets.StateAbsent,
			},
		}, after)
		_, err := ti.lm.WaitForOneVersion(ctx, id, retry.Options{})
		require.NoError(t, err)
		ti.tsql.Exec(t, "INSERT INTO db.foo VALUES (1, 1)")
	})

}
