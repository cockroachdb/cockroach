// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scexec_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type testInfra struct {
	s        serverutils.ApplicationLayerInterface
	nodeID   roachpb.NodeID
	settings *cluster.Settings
	lm       *lease.Manager
	tsql     *sqlutils.SQLRunner
	cf       *descs.CollectionFactory
	db       descs.DB
}

func (ti testInfra) newExecDeps(txn descs.Txn) scexec.Dependencies {
	const kvTrace = true
	const schemaChangerJobID = 1
	return scdeps.NewExecutorDependencies(
		ti.settings,
		ti.lm.Codec(),
		&sessiondata.SessionData{},
		txn,
		username.RootUserName(),
		txn.Descriptors(),
		noopJobRegistry{},
		noopBackfiller{},
		noopIndexSpanSplitter{},
		noopMerger{},
		scdeps.NewNoOpBackfillerTracker(ti.lm.Codec()),
		scdeps.NewNoopPeriodicProgressFlusher(),
		noopValidator{},
		scdeps.NewConstantClock(timeutil.Now()),
		noopMetadataUpdater{},
		noopTemporarySchemaCreator{},
		noopStatsReferesher{},
		&scexec.TestingKnobs{},
		kvTrace,
		schemaChangerJobID,
		nil, /* statements */
	)
}

func setupTestInfra(t testing.TB) (_ *testInfra, cleanup func(context.Context)) {
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	tt := s.ApplicationLayer()
	return &testInfra{
		s:        tt,
		nodeID:   s.NodeID(),
		settings: tt.ClusterSettings(),
		db:       tt.ExecutorConfig().(sql.ExecutorConfig).InternalDB,
		lm:       s.LeaseManager().(*lease.Manager),
		cf:       tt.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory,
		tsql:     sqlutils.MakeSQLRunner(db),
	}, s.Stopper().Stop
}

func TestExecutorDescriptorMutationOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	run := func(t *testing.T, c testCase) {
		ctx := context.Background()
		ti, cleanup := setupTestInfra(t)
		defer cleanup(ctx)

		ti.tsql.Exec(t, `CREATE DATABASE db`)
		ti.tsql.Exec(t, `
CREATE TABLE db.t (
   i INT PRIMARY KEY,
   CONSTRAINT check_foo CHECK (i > 0)
)`)

		tn := tree.MakeTableNameWithSchema("db", catconstants.PublicSchemaName, "t")
		require.NoError(t, ti.db.DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) (err error) {
			if _, table, err = descs.PrefixAndMutableTable(ctx, txn.Descriptors().MutableByName(txn.KV()), &tn); err != nil {
				return err
			}
			return nil
		}))

		require.NoError(t, ti.db.DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) error {
			exDeps := ti.newExecDeps(txn)
			_, orig, err := descs.PrefixAndTable(ctx, txn.Descriptors().ByName(txn.KV()).Get(), &tn)
			require.NoError(t, err)
			require.Equal(t, c.orig().TableDesc(), orig.TableDesc())
			require.NoError(t, scexec.ExecuteStage(ctx, exDeps, scop.PostCommitPhase, c.ops()))
			_, after, err := descs.PrefixAndTable(ctx, txn.Descriptors().ByName(txn.KV()).Get(), &tn)
			require.NoError(t, err)
			require.Equal(t, c.exp().TableDesc(), after.TableDesc())
			return nil
		}))
	}

	indexToAdd := descpb.IndexDescriptor{
		ID:                2,
		Name:              tabledesc.IndexNamePlaceholder(2),
		Version:           descpb.LatestIndexDescriptorVersion,
		CreatedExplicitly: true,
		KeyColumnIDs:      []descpb.ColumnID{1},
		KeyColumnNames:    []string{"i"},
		StoreColumnNames:  []string{},
		KeyColumnDirections: []catenumpb.IndexColumn_Direction{
			catenumpb.IndexColumn_ASC,
		},
		ConstraintID:                3,
		UseDeletePreservingEncoding: true,
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
					&scop.MakeAbsentTempIndexDeleteOnly{
						Index: scpb.Index{
							TableID:      table.ID,
							IndexID:      indexToAdd.ID,
							ConstraintID: indexToAdd.ConstraintID,
						},
						IsSecondaryIndex: true,
					},
					&scop.AddColumnToIndex{
						TableID:   table.ID,
						ColumnID:  1,
						IndexID:   indexToAdd.ID,
						Kind:      scpb.IndexColumn_KEY,
						Direction: catenumpb.IndexColumn_ASC,
						Ordinal:   0,
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	t.Run("add column", func(t *testing.T) {
		ti, cleanup := setupTestInfra(t)
		defer cleanup(ctx)
		ti.tsql.Exec(t, `CREATE DATABASE db`)
		ti.tsql.Exec(t, `CREATE TABLE db.foo (i INT PRIMARY KEY)`)

		var cs scpb.CurrentState
		require.NoError(t, ti.db.DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) (err error) {
			tn := tree.MakeTableNameWithSchema("db", catconstants.PublicSchemaName, "foo")
			_, fooTable, err := descs.PrefixAndTable(ctx, txn.Descriptors().ByNameWithLeased(txn.KV()).Get(), &tn)
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
					&scpb.Column{
						TableID:        fooTable.GetID(),
						ColumnID:       2,
						PgAttributeNum: 2,
					},
					metadata,
				),
				scpb.MakeTarget(
					scpb.ToPublic,
					&scpb.ColumnType{
						TableID:                 fooTable.GetID(),
						ColumnID:                2,
						TypeT:                   scpb.TypeT{Type: types.Int},
						IsNullable:              true,
						ElementCreationMetadata: scdecomp.NewElementCreationMetadata(clusterversion.TestingClusterVersion),
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
					&scpb.IndexColumn{
						TableID:       fooTable.GetID(),
						IndexID:       1,
						ColumnID:      2,
						OrdinalInKind: 0,
						Kind:          scpb.IndexColumn_STORED,
					},
					metadata,
				),
				scpb.MakeTarget(
					scpb.ToPublic,
					&scpb.TableData{
						TableID:    fooTable.GetID(),
						DatabaseID: fooTable.GetParentID(),
					},
					metadata,
				),
			}
			initial := []scpb.Status{
				scpb.Status_ABSENT,
				scpb.Status_ABSENT,
				scpb.Status_ABSENT,
				scpb.Status_ABSENT,
				scpb.Status_PUBLIC,
			}
			cs = scpb.CurrentState{
				TargetState: scpb.TargetState{Statements: stmts, Targets: targets},
				Initial:     initial,
				Current:     append([]scpb.Status{}, initial...),
			}
			sc := sctestutils.MakePlan(t, cs, scop.PreCommitPhase, nil /* memAcc */)
			stages := sc.StagesForCurrentPhase()
			for _, s := range stages {
				exDeps := ti.newExecDeps(txn)
				require.NoError(t, sc.DecorateErrorWithPlanDetails(scexec.ExecuteStage(ctx, exDeps, scop.PostCommitPhase, s.Ops())))
				cs = cs.WithCurrentStatuses(s.After)
			}
			return nil
		}))
		var after scpb.CurrentState
		require.NoError(t, ti.db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			sc := sctestutils.MakePlan(t, cs, scop.PostCommitPhase, nil /* memAcc */)
			for _, s := range sc.Stages {
				exDeps := ti.newExecDeps(txn)
				require.NoError(t, sc.DecorateErrorWithPlanDetails(scexec.ExecuteStage(ctx, exDeps, scop.PostCommitPhase, s.Ops())))
				after = cs.WithCurrentStatuses(s.After)
			}
			return nil
		}))
		require.Equal(t, []scpb.Status{
			scpb.Status_PUBLIC,
			scpb.Status_PUBLIC,
			scpb.Status_PUBLIC,
			scpb.Status_PUBLIC,
			scpb.Status_PUBLIC,
		}, after.Current)
		ti.tsql.Exec(t, "INSERT INTO db.foo VALUES (1, 1)")
	})
	t.Run("with builder", func(t *testing.T) {
		ti, cleanup := setupTestInfra(t)
		defer cleanup(ctx)
		ti.tsql.Exec(t, `CREATE DATABASE db`)
		ti.tsql.Exec(t, `CREATE TABLE db.foo (i INT PRIMARY KEY)`)

		var cs scpb.CurrentState
		var logSchemaChangesFn scbuild.LogSchemaChangerEventsFn
		require.NoError(t, ti.db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) (err error) {
			sctestutils.WithBuilderDependenciesFromTestServer(ti.s, ti.nodeID, func(buildDeps scbuild.Dependencies) {
				parsed, err := parser.Parse("ALTER TABLE db.foo ADD COLUMN j INT")
				require.NoError(t, err)
				require.Len(t, parsed, 1)
				cs, logSchemaChangesFn, err = scbuild.Build(ctx, buildDeps, scpb.CurrentState{}, parsed[0].AST.(*tree.AlterTable), mon.NewStandaloneUnlimitedAccount())
				require.NoError(t, err)
				require.NoError(t, logSchemaChangesFn(ctx))
				{
					sc := sctestutils.MakePlan(t, cs, scop.PreCommitPhase, nil /* memAcc */)
					for _, s := range sc.StagesForCurrentPhase() {
						exDeps := ti.newExecDeps(txn)
						require.NoError(t, sc.DecorateErrorWithPlanDetails(scexec.ExecuteStage(ctx, exDeps, scop.PostCommitPhase, s.Ops())))
						cs = cs.WithCurrentStatuses(s.After)
					}
				}
			})
			return nil
		}))
		require.NoError(t, ti.db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			sc := sctestutils.MakePlan(t, cs, scop.PostCommitPhase, nil /* memAcc */)
			for _, s := range sc.Stages {
				exDeps := ti.newExecDeps(txn)
				require.NoError(t, sc.DecorateErrorWithPlanDetails(scexec.ExecuteStage(ctx, exDeps, scop.PostCommitPhase, s.Ops())))
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
	ctx context.Context, jobID jobspb.JobID, txn isql.Txn, updateFunc jobs.UpdateFn,
) error {
	return nil
}

var _ scdeps.JobRegistry = noopJobRegistry{}

func (n noopJobRegistry) MakeJobID() jobspb.JobID {
	return jobspb.InvalidJobID
}

func (n noopJobRegistry) CreateJobWithTxn(
	ctx context.Context, record jobs.Record, jobID jobspb.JobID, txn isql.Txn,
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

func (n noopBackfiller) BackfillIndexes(
	ctx context.Context,
	progress scexec.BackfillProgress,
	writer scexec.BackfillerProgressWriter,
	job *jobs.Job,
	descriptor catalog.TableDescriptor,
) error {
	return nil
}

type noopIndexSpanSplitter struct{}

var _ scexec.IndexSpanSplitter = (*noopIndexSpanSplitter)(nil)

// MaybeSplitIndexSpans will attempt to split the backfilled index span.
func (n noopIndexSpanSplitter) MaybeSplitIndexSpans(
	ctx context.Context, table catalog.TableDescriptor, indexToBackfill catalog.Index,
) error {
	return nil
}

// MaybeSplitIndexSpansForPartitioning will attempt to split the backfilled index span.
func (n noopIndexSpanSplitter) MaybeSplitIndexSpansForPartitioning(
	ctx context.Context, table catalog.TableDescriptor, indexToBackfill catalog.Index,
) error {
	return nil
}

type noopMerger struct{}

var _ scexec.Merger = (*noopMerger)(nil)

func (n noopMerger) MergeIndexes(
	ctx context.Context,
	job *jobs.Job,
	progress scexec.MergeProgress,
	writer scexec.BackfillerProgressWriter,
	descriptor catalog.TableDescriptor,
) error {
	return nil
}

type noopValidator struct{}

var _ scexec.Validator = noopValidator{}

func (noopValidator) ValidateForwardIndexes(
	ctx context.Context,
	_ *jobs.Job,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	override sessiondata.InternalExecutorOverride,
) error {
	return nil
}

func (noopValidator) ValidateInvertedIndexes(
	ctx context.Context,
	_ *jobs.Job,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	override sessiondata.InternalExecutorOverride,
) error {
	return nil
}

func (noopValidator) ValidateConstraint(
	ctx context.Context,
	tbl catalog.TableDescriptor,
	constraint catalog.Constraint,
	indexIDForValidation descpb.IndexID,
	override sessiondata.InternalExecutorOverride,
) error {
	return nil
}

type noopStatsReferesher struct{}

var _ scexec.StatsRefresher = noopStatsReferesher{}

func (noopStatsReferesher) NotifyMutation(table catalog.TableDescriptor, rowsAffected int) {
}

type noopMetadataUpdater struct{}

var _ scexec.DescriptorMetadataUpdater = noopMetadataUpdater{}

// DeleteDatabaseRoleSettings implements scexec.DescriptorMetadataUpdater.
func (noopMetadataUpdater) DeleteDatabaseRoleSettings(ctx context.Context, dbID descpb.ID) error {
	return nil
}

// DeleteScheduleID implements scexec.DescriptorMetadataUpdater.
func (noopMetadataUpdater) DeleteSchedule(ctx context.Context, scheduleID jobspb.ScheduleID) error {
	return nil
}

// UpdateTTLScheduleLabel implements scexec.DescriptorMetadataUpdater.
func (noopMetadataUpdater) UpdateTTLScheduleLabel(
	ctx context.Context, tbl *tabledesc.Mutable,
) error {
	return nil
}

type noopTemporarySchemaCreator struct{}

var _ scexec.TemporarySchemaCreator = noopTemporarySchemaCreator{}

// InsertTemporarySchema implements scexec.TemporarySchemaCreator.
func (noopTemporarySchemaCreator) InsertTemporarySchema(
	tempSchemaName string, databaseID descpb.ID, schemaID descpb.ID,
) {

}
