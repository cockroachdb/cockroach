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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraphviz"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/stretchr/testify/require"
)

type testInfra struct {
	tc       *testcluster.TestCluster
	settings *cluster.Settings
	ie       sqlutil.InternalExecutor
	db       *kv.DB
	lm       *lease.Manager
	tsql     *sqlutils.SQLRunner
	cf       *descs.CollectionFactory
}

func (ti testInfra) newExecDeps(
	txn *kv.Txn, descsCollection *descs.Collection,
) scexec.Dependencies {
	return scdeps.NewExecutorDependencies(
		ti.lm.Codec(),
		txn,
		security.RootUserName(),
		descsCollection,
		noopJobRegistry{},    /* jobRegistry */
		noopBackfiller{},     /* indexBackfiller */
		noopIndexValidator{}, /* indexValidator */
		noopCCLCallbacks{},   /* noopCCLCallbacks */
		func(ctx context.Context, txn *kv.Txn, depth int, descID descpb.ID, metadata scpb.ElementMetadata, event eventpb.EventPayload) error {
			return nil
		},
		nil, /* statements */
	)
}

func setupTestInfra(t testing.TB) *testInfra {
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	return &testInfra{
		tc:       tc,
		settings: tc.Server(0).ClusterSettings(),
		ie:       tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor),
		db:       tc.Server(0).DB(),
		lm:       tc.Server(0).LeaseManager().(*lease.Manager),
		cf:       tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).CollectionFactory,
		tsql:     sqlutils.MakeSQLRunner(tc.ServerConn(0)),
	}
}

func (ti *testInfra) txn(
	ctx context.Context,
	f func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error,
) error {
	return ti.cf.Txn(ctx, ti.ie, ti.db, f)
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
			AvoidLeased:    true,
		},
	}
	immFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			Required:    true,
			AvoidLeased: true,
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
			exDeps := ti.newExecDeps(txn, descriptors)
			_, orig, err := descriptors.GetImmutableTableByName(ctx, txn, &tn, immFlags)
			require.NoError(t, err)
			require.Equal(t, c.orig(), orig)
			require.NoError(t, scexec.ExecuteStage(ctx, exDeps, c.ops()))
			_, after, err := descriptors.GetImmutableTableByName(ctx, txn, &tn, immFlags)
			require.NoError(t, err)
			require.Equal(t, c.exp(), after)
			return nil
		}))
	}

	indexToAdd := descpb.IndexDescriptor{
		ID:                2,
		Name:              "foo",
		Version:           descpb.StrictIndexColumnIDGuaranteesVersion,
		CreatedExplicitly: true,
		KeyColumnIDs:      []descpb.ColumnID{1},
		KeyColumnNames:    []string{"i"},
		StoreColumnNames:  []string{},
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
						TableID:             table.ID,
						IndexID:             indexToAdd.ID,
						IndexName:           indexToAdd.Name,
						KeyColumnIDs:        indexToAdd.KeyColumnIDs,
						KeyColumnDirections: indexToAdd.KeyColumnDirections,
						SecondaryIndex:      true,
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
			metadata := &scpb.TargetMetadata{
				StatementID:     0,
				SubWorkID:       1,
				SourceElementID: 1}
			targetSlice = []*scpb.Target{
				scpb.NewTarget(scpb.Target_ADD, &scpb.PrimaryIndex{
					TableID:             fooTable.GetID(),
					IndexID:             2,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnDirections: []scpb.PrimaryIndex_Direction{scpb.PrimaryIndex_ASC},
					StoringColumnIDs:    []descpb.ColumnID{2},
					Unique:              true,
					Inverted:            false,
				},
					metadata),
				scpb.NewTarget(scpb.Target_ADD, &scpb.IndexName{
					TableID: fooTable.GetID(),
					IndexID: 2,
					Name:    "new_primary_key",
				},
					metadata),
				scpb.NewTarget(scpb.Target_ADD, &scpb.ColumnName{
					TableID:  fooTable.GetID(),
					ColumnID: 2,
					Name:     "j",
				},
					metadata),
				scpb.NewTarget(scpb.Target_ADD, &scpb.Column{
					TableID:        fooTable.GetID(),
					ColumnID:       2,
					Type:           types.Int,
					Nullable:       true,
					PgAttributeNum: 2,
				},
					metadata),
				scpb.NewTarget(scpb.Target_DROP, &scpb.PrimaryIndex{
					TableID:             fooTable.GetID(),
					IndexID:             1,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnDirections: []scpb.PrimaryIndex_Direction{scpb.PrimaryIndex_ASC},
					Unique:              true,
					Inverted:            false,
				},
					metadata),
				scpb.NewTarget(scpb.Target_DROP, &scpb.IndexName{
					TableID: fooTable.GetID(),
					IndexID: 1,
					Name:    "primary",
				},
					metadata),
			}

			nodes := scpb.State{
				Nodes: []*scpb.Node{
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
						Status: scpb.Status_ABSENT,
					},
					{
						Target: targetSlice[3],
						Status: scpb.Status_ABSENT,
					},
					{
						Target: targetSlice[4],
						Status: scpb.Status_PUBLIC,
					},
					{
						Target: targetSlice[5],
						Status: scpb.Status_PUBLIC,
					},
				},
				Statements: []*scpb.Statement{
					{},
				},
			}

			for _, phase := range []scop.Phase{
				scop.StatementPhase,
				scop.PreCommitPhase,
			} {
				sc := sctestutils.MakePlan(t, nodes, phase)
				stages := sc.StagesForCurrentPhase()
				for _, s := range stages {
					exDeps := ti.newExecDeps(txn, descriptors)
					require.NoError(t, scgraphviz.DecorateErrorWithPlanDetails(scexec.ExecuteStage(ctx, exDeps, s.Ops), sc))
					ts = s.After
				}
			}
			return nil
		}))
		var after scpb.State
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			sc := sctestutils.MakePlan(t, ts, scop.PostCommitPhase)
			for _, s := range sc.StagesForCurrentPhase() {
				exDeps := ti.newExecDeps(txn, descriptors)
				require.NoError(t, scgraphviz.DecorateErrorWithPlanDetails(scexec.ExecuteStage(ctx, exDeps, s.Ops), sc))
				after = s.After
			}
			return nil
		}))
		require.Equal(t, scpb.State{
			Nodes: []*scpb.Node{
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
					Status: scpb.Status_PUBLIC,
				},
				{
					Target: targetSlice[3],
					Status: scpb.Status_PUBLIC,
				},
				{
					Target: targetSlice[4],
					Status: scpb.Status_ABSENT,
				},
				{
					Target: targetSlice[5],
					Status: scpb.Status_ABSENT,
				},
			},

			Statements: []*scpb.Statement{
				{},
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
			sctestutils.WithBuilderDependenciesFromTestServer(ti.tc.Server(0), func(buildDeps scbuild.Dependencies) {
				parsed, err := parser.Parse("ALTER TABLE db.foo ADD COLUMN j INT")
				require.NoError(t, err)
				require.Len(t, parsed, 1)
				outputNodes, err := scbuild.Build(ctx, buildDeps, scpb.State{}, parsed[0].AST.(*tree.AlterTable))
				require.NoError(t, err)

				for _, phase := range []scop.Phase{
					scop.StatementPhase,
					scop.PreCommitPhase,
				} {
					sc := sctestutils.MakePlan(t, outputNodes, phase)
					for _, s := range sc.StagesForCurrentPhase() {
						exDeps := ti.newExecDeps(txn, descriptors)
						require.NoError(t, scgraphviz.DecorateErrorWithPlanDetails(scexec.ExecuteStage(ctx, exDeps, s.Ops), sc))
						ts = s.After
					}
				}
			})
			return nil
		}))
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			sc := sctestutils.MakePlan(t, ts, scop.PostCommitPhase)
			for _, s := range sc.StagesForCurrentPhase() {
				exDeps := ti.newExecDeps(txn, descriptors)
				require.NoError(t, scgraphviz.DecorateErrorWithPlanDetails(scexec.ExecuteStage(ctx, exDeps, s.Ops), sc))
			}
			return nil
		}))
		ti.tsql.Exec(t, "INSERT INTO db.foo VALUES (1, 1)")
	})
}

type noopJobRegistry struct{}

func (n noopJobRegistry) UpdateJobWithTxn(
	ctx context.Context, jobID jobspb.JobID, txn *kv.Txn, useReadLock bool, updateFunc jobs.UpdateFn,
) error {
	return nil
}

var _ scdeps.JobRegistry = noopJobRegistry{}

func (n noopJobRegistry) MakeJobID() jobspb.JobID {
	return jobspb.InvalidJobID
}

func (n noopJobRegistry) CreateJobWithTxn(
	ctx context.Context, record jobs.Record, jobID jobspb.JobID, txn *kv.Txn,
) (*jobs.Job, error) {
	return &jobs.Job{}, nil
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

type noopIndexValidator struct{}

func (noopIndexValidator) ValidateForwardIndexes(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	withFirstMutationPublic bool,
	gatherAllInvalid bool,
	override sessiondata.InternalExecutorOverride,
) error {
	return nil
}

func (noopIndexValidator) ValidateInvertedIndexes(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	gatherAllInvalid bool,
	override sessiondata.InternalExecutorOverride,
) error {
	return nil
}

type noopCCLCallbacks struct {
}

func (noopCCLCallbacks) AddPartitioning(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	indexDesc *descpb.IndexDescriptor,
	partitionFields []string,
	listPartition []*scpb.ListPartition,
	rangePartition []*scpb.RangePartitions,
	allowedNewColumnNames []tree.Name,
	allowImplicitPartitioning bool,
) error {
	return nil
}

var _ scexec.IndexBackfiller = noopBackfiller{}
var _ scexec.IndexValidator = noopIndexValidator{}
var _ scexec.Partitioner = noopCCLCallbacks{}
