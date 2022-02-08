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
	"github.com/cockroachdb/cockroach/pkg/keys"
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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	const kvTrace = true
	const schemaChangerJobID = 1
	return scdeps.NewExecutorDependencies(
		ti.lm.Codec(),
		&sessiondata.SessionData{},
		txn,
		security.RootUserName(),
		descsCollection,
		noopJobRegistry{},
		noopBackfiller{},
		scdeps.NewNoOpBackfillTracker(ti.lm.Codec()),
		scdeps.NewNoopPeriodicProgressFlusher(),
		noopIndexValidator{},
		scdeps.NewConstantClock(timeutil.Now()),
		noopMetadataUpdaterFactory{},
		noopEventLogger{},
		kvTrace,
		schemaChangerJobID,
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
		ops       func() []scop.Op
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
   i INT PRIMARY KEY,
   CONSTRAINT check_foo CHECK (i > 0)
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
		Name:              tabledesc.IndexNamePlaceholder(2),
		Version:           descpb.PrimaryIndexWithStoredColumnsVersion,
		CreatedExplicitly: true,
		KeyColumnIDs:      []descpb.ColumnID{1},
		KeyColumnNames:    []string{"i"},
		StoreColumnNames:  []string{},
		KeyColumnDirections: []descpb.IndexDescriptor_Direction{
			descpb.IndexDescriptor_ASC,
		},
		ConstraintID: 3,
	}
	for _, tc := range []testCase{
		{
			name: "add index",
			orig: makeTable(nil),
			exp: makeTable(func(mutable *tabledesc.Mutable) {
				mutable.MaybeIncrementVersion()
				mutable.NextIndexID++
				mutable.NextConstraintID++
				mutable.Mutations = append(mutable.Mutations, descpb.DescriptorMutation{
					Descriptor_: &descpb.DescriptorMutation_Index{
						Index: &indexToAdd,
					},
					State:      descpb.DescriptorMutation_DELETE_ONLY,
					Direction:  descpb.DescriptorMutation_ADD,
					MutationID: 1,
				})
				mutable.NextMutationID = 1
			}),
			ops: func() []scop.Op {
				return []scop.Op{
					&scop.MakeAddedIndexDeleteOnly{
						Index: scpb.Index{
							TableID:             table.ID,
							IndexID:             indexToAdd.ID,
							KeyColumnIDs:        []catid.ColumnID{1},
							KeyColumnDirections: []scpb.Index_Direction{scpb.Index_ASC},
						},
						IsSecondaryIndex: true,
					},
				}
			},
		},
		{
			name: "remove check constraint",
			orig: makeTable(nil),
			exp: makeTable(func(mutable *tabledesc.Mutable) {
				mutable.MaybeIncrementVersion()
				mutable.Checks = mutable.Checks[:0]
			}),
			ops: func() []scop.Op {
				return []scop.Op{
					&scop.RemoveCheckConstraint{
						TableID:      table.GetID(),
						ConstraintID: 2,
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

		var cs scpb.CurrentState
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) (err error) {
			tn := tree.MakeTableNameWithSchema("db", tree.PublicSchemaName, "foo")
			_, fooTable, err := descriptors.GetImmutableTableByName(ctx, txn, &tn, tree.ObjectLookupFlagsWithRequired())
			require.NoError(t, err)

			stmts := []scpb.Statement{
				{
					Statement: "ALTER TABLE foo ADD COLUMN j INT",
				},
			}
			metadata := &scpb.TargetMetadata{
				StatementID:     0,
				SubWorkID:       1,
				SourceElementID: 1,
			}
			targets := []scpb.Target{
				scpb.MakeTarget(
					scpb.ToPublic,
					&scpb.PrimaryIndex{
						Index: scpb.Index{
							TableID:             fooTable.GetID(),
							IndexID:             2,
							KeyColumnIDs:        []catid.ColumnID{1},
							KeyColumnDirections: []scpb.Index_Direction{scpb.Index_ASC},
							StoringColumnIDs:    []catid.ColumnID{2},
							IsUnique:            true,
							SourceIndexID:       1,
						},
					},
					metadata,
				),
				scpb.MakeTarget(
					scpb.ToPublic,
					&scpb.IndexName{
						TableID: fooTable.GetID(),
						IndexID: 2,
						Name:    "new_primary_key",
					},
					metadata,
				),
				scpb.MakeTarget(
					scpb.ToPublic,
					&scpb.ColumnName{
						TableID:  fooTable.GetID(),
						ColumnID: 2,
						Name:     "j",
					},
					metadata,
				),
				scpb.MakeTarget(
					scpb.ToPublic,
					&scpb.ColumnType{
						TableID:    fooTable.GetID(),
						ColumnID:   2,
						TypeT:      scpb.TypeT{Type: types.Int},
						IsNullable: true,
					},
					metadata,
				),
				scpb.MakeTarget(
					scpb.ToPublic,
					&scpb.Column{
						TableID:        fooTable.GetID(),
						ColumnID:       2,
						PgAttributeNum: 2,
					},
					metadata,
				),
				scpb.MakeTarget(
					scpb.ToAbsent,
					&scpb.PrimaryIndex{
						Index: scpb.Index{
							TableID:             fooTable.GetID(),
							IndexID:             1,
							KeyColumnIDs:        []catid.ColumnID{1},
							KeyColumnDirections: []scpb.Index_Direction{scpb.Index_ASC},
							IsUnique:            true,
						},
					},
					metadata,
				),
				scpb.MakeTarget(
					scpb.ToAbsent,
					&scpb.IndexName{
						TableID: fooTable.GetID(),
						IndexID: 1,
						Name:    "primary",
					},
					metadata,
				),
			}
			current := []scpb.Status{
				scpb.Status_ABSENT,
				scpb.Status_ABSENT,
				scpb.Status_ABSENT,
				scpb.Status_ABSENT,
				scpb.Status_ABSENT,
				scpb.Status_PUBLIC,
				scpb.Status_PUBLIC,
			}
			initial := scpb.CurrentState{
				TargetState: scpb.TargetState{Statements: stmts, Targets: targets},
				Current:     current,
			}

			for _, phase := range []scop.Phase{
				scop.StatementPhase,
				scop.PreCommitPhase,
			} {
				sc := sctestutils.MakePlan(t, initial, phase)
				stages := sc.StagesForCurrentPhase()
				for _, s := range stages {
					exDeps := ti.newExecDeps(txn, descriptors)
					require.NoError(t, sc.DecorateErrorWithPlanDetails(scexec.ExecuteStage(ctx, exDeps, s.Ops())))
					cs = scpb.CurrentState{TargetState: initial.TargetState, Current: s.After}
				}
			}
			return nil
		}))
		var after scpb.CurrentState
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			sc := sctestutils.MakePlan(t, cs, scop.PostCommitPhase)
			for _, s := range sc.Stages {
				exDeps := ti.newExecDeps(txn, descriptors)
				require.NoError(t, sc.DecorateErrorWithPlanDetails(scexec.ExecuteStage(ctx, exDeps, s.Ops())))
				after = scpb.CurrentState{TargetState: cs.TargetState, Current: s.After}
			}
			return nil
		}))
		require.Equal(t, []scpb.Status{
			scpb.Status_PUBLIC,
			scpb.Status_PUBLIC,
			scpb.Status_PUBLIC,
			scpb.Status_PUBLIC,
			scpb.Status_PUBLIC,
			scpb.Status_ABSENT,
			scpb.Status_ABSENT,
		}, after.Current)
		ti.tsql.Exec(t, "INSERT INTO db.foo VALUES (1, 1)")
	})
	t.Run("with builder", func(t *testing.T) {
		ti := setupTestInfra(t)
		defer ti.tc.Stopper().Stop(ctx)
		ti.tsql.Exec(t, `CREATE DATABASE db`)
		ti.tsql.Exec(t, `CREATE TABLE db.foo (i INT PRIMARY KEY)`)

		var cs scpb.CurrentState
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) (err error) {
			sctestutils.WithBuilderDependenciesFromTestServer(ti.tc.Server(0), func(buildDeps scbuild.Dependencies) {
				parsed, err := parser.Parse("ALTER TABLE db.foo ADD COLUMN j INT")
				require.NoError(t, err)
				require.Len(t, parsed, 1)
				initial, err := scbuild.Build(ctx, buildDeps, scpb.CurrentState{}, parsed[0].AST.(*tree.AlterTable))
				require.NoError(t, err)

				for _, phase := range []scop.Phase{
					scop.StatementPhase,
					scop.PreCommitPhase,
				} {
					sc := sctestutils.MakePlan(t, initial, phase)
					for _, s := range sc.StagesForCurrentPhase() {
						exDeps := ti.newExecDeps(txn, descriptors)
						require.NoError(t, sc.DecorateErrorWithPlanDetails(scexec.ExecuteStage(ctx, exDeps, s.Ops())))
						cs = scpb.CurrentState{TargetState: initial.TargetState, Current: s.After}
					}
				}
			})
			return nil
		}))
		require.NoError(t, ti.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			sc := sctestutils.MakePlan(t, cs, scop.PostCommitPhase)
			for _, s := range sc.Stages {
				exDeps := ti.newExecDeps(txn, descriptors)
				require.NoError(t, sc.DecorateErrorWithPlanDetails(scexec.ExecuteStage(ctx, exDeps, s.Ops())))
			}
			return nil
		}))
		ti.tsql.Exec(t, "INSERT INTO db.foo VALUES (1, 1)")
	})
}

type noopJobRegistry struct{}

func (n noopJobRegistry) CheckPausepoint(name string) error {
	return nil
}

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

var _ scexec.Backfiller = (*noopBackfiller)(nil)

func (n noopBackfiller) MaybePrepareDestIndexesForBackfill(
	ctx context.Context, progress scexec.BackfillProgress, descriptor catalog.TableDescriptor,
) (scexec.BackfillProgress, error) {
	return progress, nil
}

func (n noopBackfiller) BackfillIndex(
	ctx context.Context,
	progress scexec.BackfillProgress,
	writer scexec.BackfillProgressWriter,
	descriptor catalog.TableDescriptor,
) error {
	return nil
}

type noopIndexValidator struct{}

func (noopIndexValidator) ValidateForwardIndexes(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	override sessiondata.InternalExecutorOverride,
) error {
	return nil
}

func (noopIndexValidator) ValidateInvertedIndexes(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	override sessiondata.InternalExecutorOverride,
) error {
	return nil
}

type noopEventLogger struct{}

func (noopEventLogger) LogEvent(
	_ context.Context, _ descpb.ID, _ eventpb.CommonSQLEventDetails, _ eventpb.EventPayload,
) error {
	return nil
}

type noopMetadataUpdaterFactory struct {
}

type noopMetadataUpdater struct {
}

// NewMetadataUpdater implements scexec.DescriptorMetadataUpdaterFactory.
func (noopMetadataUpdaterFactory) NewMetadataUpdater(
	ctx context.Context, txn *kv.Txn, sessionData *sessiondata.SessionData,
) scexec.DescriptorMetadataUpdater {
	return &noopMetadataUpdater{}
}

// UpsertDescriptorComment implements scexec.DescriptorMetadataUpdater.
func (noopMetadataUpdater) UpsertDescriptorComment(
	id int64, subID int64, commentType keys.CommentType, comment string,
) error {
	return nil
}

// DeleteDescriptorComment implements scexec.DescriptorMetadataUpdater.
func (noopMetadataUpdater) DeleteDescriptorComment(
	id int64, subID int64, commentType keys.CommentType,
) error {
	return nil
}

//UpsertConstraintComment implements scexec.DescriptorMetadataUpdater.
func (noopMetadataUpdater) UpsertConstraintComment(
	tableID descpb.ID, constraintID descpb.ConstraintID, comment string,
) error {
	return nil
}

//DeleteConstraintComment implements scexec.DescriptorMetadataUpdater.
func (noopMetadataUpdater) DeleteConstraintComment(
	tableID descpb.ID, constraintID descpb.ConstraintID,
) error {
	return nil
}

// DeleteDatabaseRoleSettings implements scexec.DescriptorMetadataUpdater.
func (noopMetadataUpdater) DeleteDatabaseRoleSettings(ctx context.Context, dbID descpb.ID) error {
	return nil
}

// SwapDescriptorSubComment implements  scexec.DescriptorMetadataUpdater.
func (noopMetadataUpdater) SwapDescriptorSubComment(
	id int64, oldSubID int64, newSubID int64, commentType keys.CommentType,
) error {
	return nil
}

// DeleteScheduleID implements scexec.DescriptorMetadataUpdater
func (noopMetadataUpdater) DeleteSchedule(ctx context.Context, scheduleID int64) error {
	return nil
}

var _ scexec.Backfiller = noopBackfiller{}
var _ scexec.IndexValidator = noopIndexValidator{}
var _ scexec.EventLogger = noopEventLogger{}
var _ scexec.DescriptorMetadataUpdater = noopMetadataUpdater{}
