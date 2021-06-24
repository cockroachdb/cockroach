// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scexec_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
		orig, exp func() catalog.TableDescriptor
		ops       func() scop.Ops
	}
	var table *tabledesc.Mutable
	makeTable := func(f func(mutable *tabledesc.Mutable)) func() catalog.TableDescriptor {
		return func() catalog.TableDescriptor {
			cpy := tabledesc.NewBuilder(table.TableDesc()).BuildExistingMutableTable()
			if f != nil {
				f(cpy)
			}
			return cpy.ImmutableCopy().(catalog.TableDescriptor)
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

		tn := tree.MakeTableNameWithSchema("db", tree.PublicSchemaName, "t")
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) (err error) {
			if _, table, err = descriptors.GetMutableTableByName(
				ctx, txn, &tn, mutFlags,
			); err != nil {
				return err
			}
			return nil
		}))

		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			ex := scexec.NewExecutor(txn, descriptors, ti.lm.Codec(), nil, nil, nil, nil, nil)
			_, orig, err := descriptors.GetImmutableTableByName(ctx, txn, &tn, immFlags)
			require.NoError(t, err)
			require.Equal(t, c.orig(), orig)
			require.NoError(t, ex.ExecuteOps(ctx, c.ops(), scexec.TestingKnobMetadata{}))
			_, after, err := descriptors.GetImmutableTableByName(ctx, txn, &tn, immFlags)
			require.NoError(t, err)
			require.Equal(t, c.exp(), after)
			return nil
		}))
	}

	indexToAdd := descpb.IndexDescriptor{
		ID:             2,
		Name:           "foo",
		KeyColumnIDs:   []descpb.ColumnID{1},
		KeyColumnNames: []string{"i"},
		KeyColumnDirections: []descpb.IndexDescriptor_Direction{
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
			ops: func() scop.Ops {
				return scop.MakeOps(
					&scop.MakeAddedIndexDeleteOnly{
						TableID: table.ID,
						Index:   indexToAdd,
					},
				)
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
			ops: func() scop.Ops {
				return scop.MakeOps(
					&scop.AddCheckConstraint{
						TableID:     table.GetID(),
						Name:        "check_foo",
						Expr:        "i > 1",
						ColumnIDs:   []descpb.ColumnID{1},
						Unvalidated: false,
						Hidden:      false,
					},
				)
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

		var ts scpb.State
		var targetSlice []*scpb.Target
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) (err error) {
			tn := tree.MakeTableNameWithSchema("db", tree.PublicSchemaName, "foo")
			_, fooTable, err := descriptors.GetImmutableTableByName(ctx, txn, &tn, tree.ObjectLookupFlagsWithRequired())
			require.NoError(t, err)

			// Corresponds to:
			//
			//  ALTER TABLE foo ADD COLUMN j INT;
			//
			targetSlice = []*scpb.Target{
				scpb.NewTarget(scpb.Target_ADD, &scpb.PrimaryIndex{
					TableID: fooTable.GetID(),
					Index: descpb.IndexDescriptor{
						Name:                "new_primary_key",
						ID:                  2,
						KeyColumnIDs:        []descpb.ColumnID{1},
						KeyColumnNames:      []string{"i"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						StoreColumnIDs:      []descpb.ColumnID{2},
						StoreColumnNames:    []string{"j"},
						Unique:              true,
						Type:                descpb.IndexDescriptor_FORWARD,
						Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
						EncodingType:        descpb.PrimaryIndexEncoding,
					},
					OtherPrimaryIndexID: fooTable.GetPrimaryIndexID(),
				}),
				scpb.NewTarget(scpb.Target_ADD, &scpb.Column{
					TableID:    fooTable.GetID(),
					FamilyID:   descpb.FamilyID(0),
					FamilyName: "primary",
					Column: descpb.ColumnDescriptor{
						Name:           "j",
						ID:             2,
						Type:           types.Int,
						Nullable:       true,
						PGAttributeNum: 2,
					},
				}),
				scpb.NewTarget(scpb.Target_DROP, &scpb.PrimaryIndex{
					TableID: fooTable.GetID(),
					Index: descpb.IndexDescriptor{
						Name:                "primary",
						ID:                  1,
						KeyColumnIDs:        []descpb.ColumnID{1},
						KeyColumnNames:      []string{"i"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						Unique:              true,
						Type:                descpb.IndexDescriptor_FORWARD,
					},
					OtherPrimaryIndexID: 2,
				}),
			}

			nodes := scpb.State{
				{
					Target: targetSlice[0],
					Status: scpb.Status_ABSENT,
				},
				{
					Target: targetSlice[1],
					Status: scpb.Status_ABSENT,
				},
				{
					Target: targetSlice[2],
					Status: scpb.Status_PUBLIC,
				},
			}

			for _, phase := range []scplan.Phase{
				scplan.StatementPhase,
				scplan.PreCommitPhase,
			} {
				sc, err := scplan.MakePlan(nodes, scplan.Params{
					ExecutionPhase: phase,
				})
				require.NoError(t, err)
				stages := sc.Stages
				for _, s := range stages {
					exec := scexec.NewExecutor(
						txn,
						descriptors,
						ti.lm.Codec(),
						noopBackfiller{},
						nil,
						nil,
						nil,
						nil,
					)
					require.NoError(t, exec.ExecuteOps(ctx, s.Ops, scexec.TestingKnobMetadata{}))
					ts = s.After
				}
			}
			return nil
		}))
		var after scpb.State
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			sc, err := scplan.MakePlan(ts, scplan.Params{
				ExecutionPhase: scplan.PostCommitPhase,
			})
			require.NoError(t, err)
			for _, s := range sc.Stages {
				exec := scexec.NewExecutor(txn, descriptors, ti.lm.Codec(), noopBackfiller{}, nil, nil, nil, nil)
				require.NoError(t, exec.ExecuteOps(ctx, s.Ops, scexec.TestingKnobMetadata{}))
				after = s.After
			}
			return nil
		}))
		require.Equal(t, scpb.State{
			{
				Target: targetSlice[0],
				Status: scpb.Status_PUBLIC,
			},
			{
				Target: targetSlice[1],
				Status: scpb.Status_PUBLIC,
			},
			{
				Target: targetSlice[2],
				Status: scpb.Status_ABSENT,
			},
		}, after)
		ti.tsql.Exec(t, "INSERT INTO db.foo VALUES (1, 1)")
	})
	t.Run("with builder", func(t *testing.T) {
		ti := setupTestInfra(t)
		defer ti.tc.Stopper().Stop(ctx)
		ti.tsql.Exec(t, `CREATE DATABASE db`)
		ti.tsql.Exec(t, `CREATE TABLE db.foo (i INT PRIMARY KEY)`)

		var ts scpb.State
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) (err error) {
			execCfg := ti.tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
			ip, cleanup := sql.NewInternalPlanner(
				"foo",
				kv.NewTxn(context.Background(), ti.db, ti.tc.Server(0).NodeID()),
				security.RootUserName(),
				&sql.MemoryMetrics{},
				&execCfg,
				sessiondatapb.SessionData{},
			)
			planner := ip.(interface {
				resolver.SchemaResolver
				SemaCtx() *tree.SemaContext
				EvalContext() *tree.EvalContext
				scbuild.AuthorizationAccessor
			})
			defer cleanup()
			buildDeps := scbuild.Dependencies{
				Res:          planner,
				SemaCtx:      planner.SemaCtx(),
				EvalCtx:      planner.EvalContext(),
				Descs:        descriptors,
				AuthAccessor: planner,
			}
			parsed, err := parser.Parse("ALTER TABLE db.foo ADD COLUMN j INT")
			require.NoError(t, err)
			require.Len(t, parsed, 1)
			outputNodes, err := scbuild.Build(ctx, buildDeps, nil, parsed[0].AST.(*tree.AlterTable))
			require.NoError(t, err)

			for _, phase := range []scplan.Phase{
				scplan.StatementPhase,
				scplan.PreCommitPhase,
			} {
				sc, err := scplan.MakePlan(outputNodes, scplan.Params{
					ExecutionPhase: phase,
				})
				require.NoError(t, err)
				for _, s := range sc.Stages {
					require.NoError(t, scexec.NewExecutor(txn, descriptors, ti.lm.Codec(), noopBackfiller{}, nil, nil, nil, nil).
						ExecuteOps(ctx, s.Ops, scexec.TestingKnobMetadata{}))
					ts = s.After
				}
			}
			return nil
		}))
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			sc, err := scplan.MakePlan(ts, scplan.Params{
				ExecutionPhase: scplan.PostCommitPhase,
			})
			require.NoError(t, err)
			for _, s := range sc.Stages {
				exec := scexec.NewExecutor(txn, descriptors, ti.lm.Codec(), noopBackfiller{}, nil, nil, nil, nil)
				require.NoError(t, exec.ExecuteOps(ctx, s.Ops, scexec.TestingKnobMetadata{}))
			}
			return nil
		}))
		ti.tsql.Exec(t, "INSERT INTO db.foo VALUES (1, 1)")
	})
}

type noopBackfiller struct{}

func (n noopBackfiller) BackfillIndex(
	ctx context.Context,
	_ scexec.JobProgressTracker,
	_ catalog.TableDescriptor,
	source descpb.IndexID,
	destinations ...descpb.IndexID,
) error {
	return nil
}

var _ scexec.IndexBackfiller = noopBackfiller{}
