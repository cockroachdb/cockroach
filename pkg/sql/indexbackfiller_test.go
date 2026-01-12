// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestIndexBackfiller tests the scenarios described in docs/tech-notes/index-backfill.md
func TestIndexBackfiller(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var params base.TestServerArgs

	moveToTDelete := make(chan bool)
	moveToTWrite := make(chan bool)
	moveToTScan := make(chan bool)
	moveToBackfill := make(chan bool)

	moveToTMerge := make(chan bool)
	backfillDone := make(chan bool)

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforePublishWriteAndDelete: func() {
				// Signal that we've moved into DELETE_ONLY.
				moveToTDelete <- true
				// Wait until we get a signal to move to WRITE_ONLY.
				<-moveToTWrite
			},
			RunBeforeBackfill: func() error {
				// Wait until we get a signal to pick our scan timestamp.
				<-moveToTScan
				return nil
			},
			RunBeforeTempIndexMerge: func() {
				backfillDone <- true
				<-moveToTMerge
			},
			RunBeforeIndexBackfill: func() {
				// Wait until we get a signal to begin backfill.
				<-moveToBackfill
			},
		},
	}

	tc := serverutils.StartCluster(t, 3,
		base.TestClusterArgs{
			ServerArgs: params,
		})
	defer tc.Stopper().Stop(context.Background())
	sqlDB := tc.ServerConn(0)

	execOrFail := func(query string) gosql.Result {
		if res, err := sqlDB.Exec(query); err != nil {
			t.Fatal(err)
		} else {
			return res
		}
		return nil
	}

	// The sequence of events here exactly matches the test cases in
	// docs/tech-notes/index-backfill.md. If you update this, please remember to
	// update the tech note as well.
	execOrFail("SET create_table_with_schema_locked=false")
	execOrFail("SET use_declarative_schema_changer='off'")
	execOrFail("CREATE DATABASE t")
	execOrFail("CREATE TABLE t.kv (k int PRIMARY KEY, v char)")
	execOrFail("INSERT INTO t.kv VALUES (1, 'a'), (3, 'c'), (4, 'e'), (6, 'f'), (7, 'g'), (9, 'h')")

	// Start the schema change.
	var finishedSchemaChange sync.WaitGroup
	finishedSchemaChange.Add(1)
	go func() {
		execOrFail("CREATE UNIQUE INDEX vidx on t.kv(v)")
		finishedSchemaChange.Done()
	}()

	// tempIndex: DELETE_ONLY
	// newIndex   BACKFILLING
	<-moveToTDelete
	execOrFail("DELETE FROM t.kv WHERE k=9")       // new_index: nothing, temp_index: sees delete
	execOrFail("INSERT INTO t.kv VALUES (9, 'h')") // new_index: nothing, temp_index: nothing

	// Move to WRITE_ONLY mode.
	// tempIndex: WRITE_ONLY
	// newIndex   BACKFILLING
	moveToTWrite <- true
	execOrFail("INSERT INTO t.kv VALUES (2, 'b')") // new_index: nothing, temp_index: sees insert

	// Pick our scan timestamp.
	// tempIndex: WRITE_ONLY
	// newIndex   BACKFILLING
	moveToTScan <- true
	execOrFail("UPDATE t.kv SET v = 'd' WHERE k = 3")
	execOrFail("UPDATE t.kv SET k = 5 WHERE v = 'e'")
	execOrFail("DELETE FROM t.kv WHERE k = 6")

	// Begin the backfill.
	moveToBackfill <- true

	<-backfillDone
	execOrFail("INSERT INTO t.kv VALUES (10, 'z')") // new_index: nothing, temp_index: sees insert
	moveToTMerge <- true

	finishedSchemaChange.Wait()

	pairsPrimary := queryPairs(t, sqlDB, "SELECT k, v FROM t.kv ORDER BY k ASC")
	pairsIndex := queryPairs(t, sqlDB, "SELECT k, v FROM t.kv@vidx ORDER BY k ASC")

	if len(pairsPrimary) != len(pairsIndex) {
		t.Fatalf("Mismatched entries in table and index: %+v %+v", pairsPrimary, pairsIndex)
	}

	for i, pPrimary := range pairsPrimary {
		if pPrimary != pairsIndex[i] {
			t.Fatalf("Mismatched entry in table and index: %+v %+v", pPrimary, pairsIndex[i])
		}
	}
}

type pair struct {
	k int
	v string
}

func queryPairs(t *testing.T, sqlDB *gosql.DB, query string) []pair {
	rows, err := sqlDB.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	ret := make([]pair, 0)
	for rows.Next() {
		p := pair{}
		if err := rows.Scan(&p.k, &p.v); err != nil {
			t.Fatal(err)
		}
		ret = append(ret, p)
	}
	return ret
}

// TestIndexBackfillerComputedAndGeneratedColumns tests that the index
// backfiller support backfilling columns with default values or computed
// expressions which are being backfilled.
//
// This is a remarkably low-level test. It manually mucks with the table
// descriptor to set up an index backfill and then manually mucks sets up and
// plans the index backfill, then it mucks with the table descriptor more
// and ensures that the data was backfilled properly.
func TestIndexBackfillerComputedAndGeneratedColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// A test case exists to exercise the behavior of an index backfill.
	// The case gets to do arbitrary things to the table (which is created with
	// the specified tableName in setupSQL). The idea is that there will be a
	// mutation on the table with mutation ID 1 that represents an index backfill.
	// The associated job (which is created by the test harness for the first
	// mutation), will be blocked from running. Instead, the test will manually
	// run the index backfiller via a hook exposed only for testing.
	//
	// After the backfill is run, the index (specified by its ID) is scanned and
	// the contents are compared with expectedContents.
	type indexBackfillTestCase struct {
		// name of the test case.
		name string

		// SQL statements to run to setup the table.
		setupSQL  string
		tableName string

		// setupDesc should mutate the descriptor such that the mutation with
		// id 1 contains an index backfill.
		setupDesc        func(t *testing.T, ctx context.Context, mut *tabledesc.Mutable, settings *cluster.Settings)
		indexToBackfill  descpb.IndexID
		expectedContents [][]string
	}

	indexBackfillerTestCases := []indexBackfillTestCase{
		// This tests a secondary index which uses a virtual computed column in its
		// key.
		{
			name: "virtual computed column in key",
			setupSQL: `
CREATE TABLE foo (i INT PRIMARY KEY, k INT, v INT AS (i*i + k) VIRTUAL);
INSERT INTO foo VALUES (1, 2), (2, 3), (3, 4);
`,
			tableName:       "foo",
			indexToBackfill: 2,
			// Note that the results are ordered by column ID.
			expectedContents: [][]string{
				{"1", "3"},
				{"2", "7"},
				{"3", "13"},
			},
			setupDesc: func(t *testing.T, ctx context.Context, mut *tabledesc.Mutable, settings *cluster.Settings) {
				indexToBackfill := descpb.IndexDescriptor{
					Name:         "virtual_column_backed_index",
					ID:           mut.NextIndexID,
					ConstraintID: mut.NextConstraintID,
					Unique:       true,
					Version:      descpb.LatestIndexDescriptorVersion,
					KeyColumnNames: []string{
						mut.Columns[2].Name,
					},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{
						catenumpb.IndexColumn_ASC,
					},
					KeyColumnIDs: []descpb.ColumnID{
						mut.Columns[2].ID,
					},
					KeySuffixColumnIDs: []descpb.ColumnID{
						mut.Columns[0].ID,
					},
					Type:         idxtype.FORWARD,
					EncodingType: catenumpb.SecondaryIndexEncoding,
				}
				mut.NextIndexID++
				mut.NextConstraintID++
				require.NoError(t, mut.AddIndexMutationMaybeWithTempIndex(&indexToBackfill, descpb.DescriptorMutation_ADD))
				require.NoError(t, mut.AllocateIDs(context.Background(), settings.Version.ActiveVersion(ctx)))
			},
		},
		// This test will inject a new primary index and perform a primary key swap
		// where the new primary index has two new stored columns not in the existing
		// primary index.
		{
			name: "default and generated column referencing it in new primary index",
			setupSQL: `
CREATE TABLE foo (i INT PRIMARY KEY);
INSERT INTO foo VALUES (1), (10), (100);
`,
			tableName:       "foo",
			indexToBackfill: 2,
			expectedContents: [][]string{
				{"1", "42", "43"},
				{"10", "42", "52"},
				{"100", "42", "142"},
			},
			setupDesc: func(t *testing.T, ctx context.Context, mut *tabledesc.Mutable, settings *cluster.Settings) {
				columnWithDefault := descpb.ColumnDescriptor{
					Name:           "def",
					ID:             mut.NextColumnID,
					Type:           types.Int,
					Nullable:       false,
					DefaultExpr:    proto.String("42"),
					Hidden:         false,
					PGAttributeNum: descpb.PGAttributeNum(mut.NextColumnID),
				}
				mut.NextColumnID++
				mut.AddColumnMutation(&columnWithDefault, descpb.DescriptorMutation_ADD)
				// Cheat and jump right to WRITE_ONLY.
				mut.Mutations[len(mut.Mutations)-1].State = descpb.DescriptorMutation_WRITE_ONLY
				computedColumnNotInPrimaryIndex := descpb.ColumnDescriptor{
					Name:           "comp",
					ID:             mut.NextColumnID,
					Type:           types.Int,
					Nullable:       false,
					ComputeExpr:    proto.String("i + def"),
					Hidden:         false,
					PGAttributeNum: descpb.PGAttributeNum(mut.NextColumnID),
				}
				mut.NextColumnID++
				mut.AddColumnMutation(&computedColumnNotInPrimaryIndex, descpb.DescriptorMutation_ADD)
				// Cheat and jump right to WRITE_ONLY.
				mut.Mutations[len(mut.Mutations)-1].State = descpb.DescriptorMutation_WRITE_ONLY

				mut.Families[0].ColumnIDs = append(mut.Families[0].ColumnIDs,
					columnWithDefault.ID,
					computedColumnNotInPrimaryIndex.ID)
				mut.Families[0].ColumnNames = append(mut.Families[0].ColumnNames,
					columnWithDefault.Name,
					computedColumnNotInPrimaryIndex.Name)

				indexToBackfill := descpb.IndexDescriptor{
					Name:         "new_primary_index",
					ID:           mut.NextIndexID,
					ConstraintID: mut.NextConstraintID,
					Unique:       true,
					Version:      descpb.LatestIndexDescriptorVersion,
					KeyColumnNames: []string{
						mut.Columns[0].Name,
					},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{
						catenumpb.IndexColumn_ASC,
					},
					StoreColumnNames: []string{
						columnWithDefault.Name,
						computedColumnNotInPrimaryIndex.Name,
					},
					KeyColumnIDs: []descpb.ColumnID{
						mut.Columns[0].ID,
					},
					KeySuffixColumnIDs: nil,
					StoreColumnIDs: []descpb.ColumnID{
						columnWithDefault.ID,
						computedColumnNotInPrimaryIndex.ID,
					},
					Type:         idxtype.FORWARD,
					EncodingType: catenumpb.PrimaryIndexEncoding,
				}
				mut.NextIndexID++
				mut.NextConstraintID++
				require.NoError(t, mut.AddIndexMutationMaybeWithTempIndex(&indexToBackfill, descpb.DescriptorMutation_ADD))
				require.NoError(t, mut.AllocateIDs(context.Background(), settings.Version.ActiveVersion(ctx)))
				mut.AddPrimaryKeySwapMutation(&descpb.PrimaryKeySwap{
					OldPrimaryIndexId: 1,
					NewPrimaryIndexId: 2,
				})
			},
		},
	}

	// fetchIndex fetches the contents of an a table index returning the results
	// as datums. The datums will correspond to each of the columns stored in the
	// index, ordered by column ID.
	fetchIndex := func(
		ctx context.Context, t *testing.T, codec keys.SQLCodec, txn *kv.Txn, table *tabledesc.Mutable, indexID descpb.IndexID,
	) []tree.Datums {
		t.Helper()

		mm := mon.NewStandaloneBudget(1 << 30)
		idx, err := catalog.MustFindIndexByID(table, indexID)
		colIDsNeeded := idx.CollectKeyColumnIDs()
		if idx.Primary() {
			for _, column := range table.PublicColumns() {
				if !column.IsVirtual() {
					colIDsNeeded.Add(column.GetID())
				}
			}
		} else {
			colIDsNeeded.UnionWith(idx.CollectSecondaryStoredColumnIDs())
			colIDsNeeded.UnionWith(idx.CollectKeySuffixColumnIDs())
		}

		require.NoError(t, err)
		spans := []roachpb.Span{table.IndexSpan(codec, indexID)}
		var fetcherCols []descpb.ColumnID
		for _, col := range table.PublicColumns() {
			if colIDsNeeded.Contains(col.GetID()) {
				fetcherCols = append(fetcherCols, col.GetID())
			}
		}
		var alloc tree.DatumAlloc
		var spec fetchpb.IndexFetchSpec
		require.NoError(t, rowenc.InitIndexFetchSpec(
			&spec,
			codec,
			table,
			idx,
			fetcherCols,
		))
		var fetcher row.Fetcher
		require.NoError(t, fetcher.Init(
			ctx,
			row.FetcherInitArgs{
				Txn:        txn,
				Alloc:      &alloc,
				MemMonitor: mm.Monitor(),
				Spec:       &spec,
				TraceKV:    true,
			},
		))

		require.NoError(t, fetcher.StartScan(
			ctx, spans, nil /* spanIDs */, rowinfra.NoBytesLimit, 0,
		))
		var rows []tree.Datums
		for {
			datums, _, err := fetcher.NextRowDecoded(ctx)
			require.NoError(t, err)
			if datums == nil {
				break
			}
			// Copy the datums out as the slice is reused internally.
			row := append(tree.Datums(nil), datums...)
			rows = append(rows, row)
		}
		return rows
	}

	datumSliceToStrMatrix := func(rows []tree.Datums) [][]string {
		res := make([][]string, len(rows))
		for i, row := range rows {
			rowStrs := make([]string, len(row))
			for j, d := range row {
				rowStrs[j] = d.String()
			}
			res[i] = rowStrs
		}
		return res
	}

	// See the comment on indexBackfillTestCase for the behavior of run.
	run := func(t *testing.T, test indexBackfillTestCase) {
		ctx := context.Background()

		// Ensure the job doesn't actually run. The code below will handle running
		// the index backfill.
		blockChan := make(chan struct{})
		var jobToBlock atomic.Value
		jobToBlock.Store(jobspb.InvalidJobID)
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
					RunBeforeResume: func(jobID jobspb.JobID) error {
						if jobID == jobToBlock.Load().(jobspb.JobID) {
							<-blockChan
							return errors.New("boom")
						}
						return nil
					},
				},
			},
		})
		defer s.Stopper().Stop(ctx)
		defer close(blockChan)

		// Run the initial setupSQL.
		tdb := sqlutils.MakeSQLRunner(db)
		tdb.Exec(t, test.setupSQL)

		// Fetch the descriptor ID for the relevant table.
		var tableID descpb.ID
		tdb.QueryRow(t, "SELECT ($1::REGCLASS)::INT", test.tableName).Scan(&tableID)

		// Run the testCase's setupDesc function to prepare an index backfill
		// mutation. Also, create an associated job and set it up to be blocked.
		tt := s.ApplicationLayer()
		lm := tt.LeaseManager().(*lease.Manager)
		codec := tt.Codec()
		settings := tt.ClusterSettings()
		execCfg := tt.ExecutorConfig().(sql.ExecutorConfig)
		jr := tt.JobRegistry().(*jobs.Registry)
		var j *jobs.Job
		var table catalog.TableDescriptor
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) (err error) {
			mut, err := descriptors.MutableByID(txn.KV()).Table(ctx, tableID)
			if err != nil {
				return err
			}
			test.setupDesc(t, ctx, mut, settings)
			span := mut.PrimaryIndexSpan(execCfg.Codec)
			resumeSpanList := make([]jobspb.ResumeSpanList, len(mut.Mutations))
			for i := range mut.Mutations {
				resumeSpanList[i] = jobspb.ResumeSpanList{
					ResumeSpans: []roachpb.Span{span},
				}
			}
			jobID := jr.MakeJobID()
			j, err = jr.CreateAdoptableJobWithTxn(ctx, jobs.Record{
				Description:   "testing",
				Statements:    []string{"testing"},
				Username:      username.RootUserName(),
				DescriptorIDs: []descpb.ID{tableID},
				Details: jobspb.SchemaChangeDetails{
					DescID:          tableID,
					TableMutationID: 1,
					FormatVersion:   jobspb.JobResumerFormatVersion,
					ResumeSpanList:  resumeSpanList,
				},
				Progress: jobspb.SchemaChangeGCProgress{},
			}, jobID, txn)
			if err != nil {
				return err
			}
			mut.MutationJobs = append(mut.MutationJobs, descpb.TableDescriptor_MutationJob{
				JobID:      jobID,
				MutationID: 1,
			})
			jobToBlock.Store(jobID)
			mut.MaybeIncrementVersion()
			table = mut.ImmutableCopy().(catalog.TableDescriptor)
			return descriptors.WriteDesc(ctx, false /* kvTrace */, mut, txn.KV())
		}))

		// Run the index backfill
		changer := sql.NewSchemaChangerForTesting(
			tableID, 1, execCfg.NodeInfo.NodeID.SQLInstanceID(), execCfg.InternalDB, lm, jr, &execCfg, settings)
		changer.SetJob(j)
		spans := []roachpb.Span{table.IndexSpan(codec, test.indexToBackfill)}
		require.NoError(t, changer.TestingDistIndexBackfill(ctx, table.GetVersion(), spans,
			[]descpb.IndexID{test.indexToBackfill}, backfill.IndexMutationFilter))

		// Make the mutation complete, then read the index and validate that it
		// has the expected contents.
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			table, err := descriptors.MutableByID(txn.KV()).Table(ctx, tableID)
			if err != nil {
				return err
			}
			toComplete := len(table.Mutations)
			for i := 0; i < toComplete; i++ {
				mut := table.Mutations[i]
				require.NoError(t, table.MakeMutationComplete(mut))
			}
			table.Mutations = table.Mutations[toComplete:]
			datums := fetchIndex(ctx, t, codec, txn.KV(), table, test.indexToBackfill)
			require.Equal(t, test.expectedContents, datumSliceToStrMatrix(datums))
			return nil
		}))
	}

	for _, test := range indexBackfillerTestCases {
		t.Run(test.name, func(t *testing.T) { run(t, test) })
	}
}

// TestIndexBackfillerResumePreservesProgress tests that spans completed during
// a backfill are properly preserved during PAUSE/RESUMEs. In particular,
// we test the following backfill sequence (where b[n] denotes the same
// backfill, just at different points of progression):
//
//	b[1] -> PAUSE -> RESUME -> b[2] -> PAUSE -> RESUME -> b[3]
//
// Before #140358, b[2] only checkpointed the spans it has completed since the
// backfill was resumed -- leading to [b3] redoing work already completed since
// b[1].
func TestIndexBackfillerResumePreservesProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDeadlock(t, "slow timing sensitive test")
	skip.UnderRace(t, "slow timing sensitive test")

	ctx := context.Background()
	// backfillProgressCompletedCh will be used to communicate completed spans
	// with the tenant prefix removed, if applicable.
	backfillProgressCompletedCh := make(chan []roachpb.Span)
	const numRows = 100
	const numSpans = 20
	var isBlockingBackfillProgress atomic.Bool
	var codec keys.SQLCodec

	// Start the server with testing knob.
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL: &execinfra.TestingKnobs{
				// We want to push progress every batch_size rows to control
				// the backfill incrementally.
				BulkAdderFlushesEveryBatch: true,
				RunBeforeIndexBackfillProgressUpdate: func(ctx context.Context, completed []roachpb.Span) {
					if isBlockingBackfillProgress.Load() {
						if toRemove := len(codec.TenantPrefix()); toRemove > 0 {
							// Remove the tenant prefix from completed spans.
							updated := make([]roachpb.Span, len(completed))
							for i := range updated {
								sp := completed[i]
								updated[i] = roachpb.Span{
									Key:    append(roachpb.Key(nil), sp.Key[toRemove:]...),
									EndKey: append(roachpb.Key(nil), sp.EndKey[toRemove:]...),
								}
							}
							completed = updated
						}
						select {
						case <-ctx.Done():
						case backfillProgressCompletedCh <- completed:
							t.Logf("before index backfill progress update, completed spans: %v", completed)
						}
					}
				},
			},
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
				RunBeforeBackfill: func(progresses []scexec.BackfillProgress) error {
					if progresses != nil {
						t.Logf("before resuming backfill, checkpointed spans: %v", progresses[0].CompletedSpans)
					}
					return nil
				},
				AfterStage: func(p scplan.Plan, stageIdx int) error {
					if p.Stages[stageIdx].Type() != scop.BackfillType || !isBlockingBackfillProgress.Load() {
						return nil
					}
					isBlockingBackfillProgress.Store(false)
					close(backfillProgressCompletedCh)
					return nil
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	codec = s.Codec()
	isBlockingBackfillProgress.Store(true)

	_, err := db.Exec(`SET CLUSTER SETTING bulkio.index_backfill.batch_size = 10`)
	require.NoError(t, err)
	_, err = db.Exec(`SET CLUSTER SETTING bulkio.index_backfill.ingest_concurrency=4`)
	require.NoError(t, err)
	// Ensure that we checkpoint our progress to the backfill job so that
	// RESUMEs can get an up-to-date backfill progress.
	_, err = db.Exec(`SET CLUSTER SETTING bulkio.index_backfill.checkpoint_interval = '10ms'`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE t(i INT PRIMARY KEY)`)
	require.NoError(t, err)
	// Have a 100 splits each containing a 100 rows.
	_, err = db.Exec(`INSERT INTO t SELECT generate_series(1, $1)`, (numRows*numSpans)+1)
	require.NoError(t, err)
	for split := 0; split < numSpans; split++ {
		_, err = db.Exec(`ALTER TABLE t SPLIT AT VALUES ($1)`, numRows*numSpans)
	}
	require.NoError(t, err)
	var descID catid.DescID
	descIDRow := db.QueryRow(`SELECT 't'::regclass::oid`)
	err = descIDRow.Scan(&descID)
	require.NoError(t, err)

	var jobID int
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		_, err := db.Exec(`ALTER TABLE t ADD COLUMN j INT NOT NULL DEFAULT 42`)
		if err != nil && err.Error() != fmt.Sprintf("pq: job %d was paused before it completed", jobID) {
			return err
		}
		return nil
	})

	testutils.SucceedsWithin(t, func() error {
		jobIDRow := db.QueryRow(`
				SELECT job_id FROM [SHOW JOBS]
				WHERE job_type = 'NEW SCHEMA CHANGE' AND description ILIKE '%ADD COLUMN j%'`,
		)
		if err := jobIDRow.Scan(&jobID); err != nil {
			return err
		}
		return nil
	}, 30*time.Second)

	ensureJobState := func(targetState string) {
		testutils.SucceedsWithin(t, func() error {
			var jobState string
			statusRow := db.QueryRow(`SELECT status FROM [SHOW JOB $1]`, jobID)
			if err := statusRow.Scan(&jobState); err != nil {
				return err
			}
			if jobState != targetState {
				return errors.Errorf("expected job to be %s, but found status: %s",
					targetState, jobState)
			}
			return nil
		}, 30*time.Second)
	}

	var completedSpans roachpb.SpanGroup
	var observedSpans []roachpb.Span
	receiveProgressUpdate := func() {
		updateCount := 2
		for isBlockingBackfillProgress.Load() && updateCount > 0 {
			progressUpdate := <-backfillProgressCompletedCh
			// Make sure the progress update does not contain overlapping spans.
			for i, span1 := range progressUpdate {
				for j, span2 := range progressUpdate {
					if i == j {
						continue
					}
					if span1.Overlaps(span2) {
						t.Fatalf("progress update contains overlapping spans: %s and %s", span1, span2)
					}
				}
			}
			hasMoreSpans := completedSpans.Add(progressUpdate...)
			if hasMoreSpans {
				updateCount -= 1
			}
			observedSpans = append(observedSpans, progressUpdate...)
		}
	}

	ensureCompletedSpansAreCheckpointed := func() {
		testutils.SucceedsWithin(t, func() error {
			stmt := `SELECT payload FROM crdb_internal.system_jobs WHERE id = $1`
			var payloadBytes []byte
			if err := db.QueryRowContext(ctx, stmt, jobID).Scan(&payloadBytes); err != nil {
				return err
			}

			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err != nil {
				return err
			}

			schemaChangeProgress := *(payload.Details.(*jobspb.Payload_NewSchemaChange).NewSchemaChange)
			var checkpointedSpans []roachpb.Span
			if len(schemaChangeProgress.BackfillProgress) > 0 {
				checkpointedSpans = schemaChangeProgress.BackfillProgress[0].CompletedSpans
			}
			var sg roachpb.SpanGroup
			sg.Add(checkpointedSpans...)
			// Ensure that the spans we already completed are fully contained in our
			// checkpointed completed spans group.
			if !sg.Encloses(completedSpans.Slice()...) {
				return errors.Errorf("checkpointed spans %v do not enclose completed spans %v",
					checkpointedSpans, completedSpans.Slice())
			}

			return nil
		}, 30*time.Second)
	}

	for isBlockingBackfillProgress.Load() {
		receiveProgressUpdate()
		ensureCompletedSpansAreCheckpointed()
		t.Logf("pausing backfill")
		_, err = db.Exec(`PAUSE JOB $1`, jobID)
		require.NoError(t, err)
		ensureJobState("paused")

		t.Logf("resuming backfill")
		_, err = db.Exec(`RESUME JOB $1`, jobID)
		require.NoError(t, err)
		ensureJobState("running")
	}
	// Now we can wait for the job to succeed
	ensureJobState("succeeded")
	if err = g.Wait(); err != nil {
		require.NoError(t, err)
	}
	// Make sure the spans we are adding do not overlap otherwise, this indicates
	// a bug. Where we computed chunks incorrectly. Each chunk should be an independent
	// piece of work.
	for i, span1 := range observedSpans {
		for j, span2 := range observedSpans {
			if i == j {
				continue
			}
			if span1.Overlaps(span2) {
				t.Fatalf("progress update contains overlapping spans: %s and %s", span1, span2)
			}
		}
	}
}

func TestDistributedMergeStoragePrefixTracking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create a temp directory for nodelocal storage used by distributed merge.
	tempDir := t.TempDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: tempDir,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(db)

	tdb.Exec(t, `SET CLUSTER SETTING bulkio.index_backfill.distributed_merge.mode = 'declarative'`)
	tdb.Exec(t, `CREATE TABLE t (k INT PRIMARY KEY, v INT)`)
	tdb.Exec(t, `INSERT INTO t SELECT i, i*10 FROM generate_series(1, 100) AS g(i)`)

	// Create a non-unique index. This triggers the distributed merge pipeline.
	tdb.Exec(t, `CREATE INDEX idx ON t (v)`)

	// Find the schema change job that was created.
	var jobID int64
	tdb.QueryRow(t, `
		SELECT job_id FROM crdb_internal.jobs
		WHERE job_type = 'NEW SCHEMA CHANGE'
		AND description LIKE '%CREATE INDEX idx%'
		ORDER BY created DESC
		LIMIT 1
	`).Scan(&jobID)
	require.NotZero(t, jobID, "expected to find schema change job")

	// Read the job payload to check for storage prefixes.
	var payloadBytes []byte
	tdb.QueryRow(t, `SELECT payload FROM crdb_internal.system_jobs WHERE id = $1`, jobID).Scan(&payloadBytes)

	payload := &jobspb.Payload{}
	require.NoError(t, protoutil.Unmarshal(payloadBytes, payload))
	schemaChangeDetails := payload.Details.(*jobspb.Payload_NewSchemaChange).NewSchemaChange
	require.NotNil(t, schemaChangeDetails)
	require.NotEmpty(t, schemaChangeDetails.BackfillProgress)
	backfillProgress := schemaChangeDetails.BackfillProgress[0]
	require.NotEmpty(t, backfillProgress.SSTStoragePrefixes)

	// Verify the prefix format matches nodelocal://<nodeID>/.
	for _, prefix := range backfillProgress.SSTStoragePrefixes {
		t.Logf("found storage prefix: %s", prefix)
		require.Regexp(t, `^nodelocal://\d+/$`, prefix,
			"storage prefix should match nodelocal://<nodeID>/ format")
	}
}

// TestDistributedMergeStoragePrefixPreservedAcrossPauseResume verifies that
// storage prefixes recorded during the distributed merge pipeline are preserved
// across job pause/resume cycles. This ensures cleanup can find all temporary
// SST files even after the job is paused and resumed.
func TestDistributedMergeStoragePrefixPreservedAcrossPauseResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tempDir := t.TempDir()

	// Coordination state for pausing after first merge iteration.
	var iterationCompleted atomic.Int32
	iterationCh := make(chan struct{})

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: tempDir,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL: &execinfra.TestingKnobs{
				BulkAdderFlushesEveryBatch: true,
				AfterDistributedMergeIteration: func(ctx context.Context, iteration int, manifests []jobspb.IndexBackfillSSTManifest) {
					iterationCompleted.Store(int32(iteration))
					// Block after iteration 1 to allow test to pause the job.
					// Only block if there are manifests (not the final KV ingest iteration).
					if iteration == 1 && len(manifests) > 0 {
						select {
						case <-iterationCh:
						case <-ctx.Done():
						}
					}
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(db)

	tdb.Exec(t, `SET CLUSTER SETTING bulkio.index_backfill.distributed_merge.mode = 'declarative'`)
	tdb.Exec(t, `SET CLUSTER SETTING bulkio.index_backfill.distributed_merge.iterations = 4`)
	tdb.Exec(t, `SET CLUSTER SETTING bulkio.index_backfill.checkpoint_interval = '10ms'`)
	tdb.Exec(t, `SET CLUSTER SETTING bulkio.merge.file_size = '50KiB'`)

	tdb.Exec(t, `CREATE TABLE t (k INT PRIMARY KEY, v TEXT)`)
	tdb.Exec(t, `INSERT INTO t SELECT i, repeat('x', 100) || i::text FROM generate_series(1, 500) AS g(i)`)

	// Helper to extract storage prefixes from job payload.
	getStoragePrefixes := func(jobID int64) []string {
		var payloadBytes []byte
		tdb.QueryRow(t, `SELECT payload FROM crdb_internal.system_jobs WHERE id = $1`, jobID).Scan(&payloadBytes)
		payload := &jobspb.Payload{}
		require.NoError(t, protoutil.Unmarshal(payloadBytes, payload))
		details := payload.Details.(*jobspb.Payload_NewSchemaChange).NewSchemaChange
		if len(details.BackfillProgress) == 0 {
			return nil
		}
		return details.BackfillProgress[0].SSTStoragePrefixes
	}

	// Run CREATE INDEX in background.
	var jobID int64
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		_, err := db.ExecContext(ctx, `CREATE INDEX idx ON t (v)`)
		if err == nil {
			return nil
		}
		if err.Error() == fmt.Sprintf("pq: job %d was paused before it completed", jobID) {
			return nil //nolint:returnerrcheck
		}
		return err
	})

	// Wait for the job to be created.
	testutils.SucceedsWithin(t, func() error {
		row := db.QueryRow(`
			SELECT job_id FROM crdb_internal.jobs
			WHERE job_type = 'NEW SCHEMA CHANGE'
			AND description LIKE '%CREATE INDEX idx%'
			ORDER BY created DESC LIMIT 1
		`)
		return row.Scan(&jobID)
	}, 30*time.Second)

	// Wait for iteration 1 to complete.
	testutils.SucceedsWithin(t, func() error {
		if iterationCompleted.Load() >= 1 {
			return nil
		}
		return errors.New("waiting for iteration 1")
	}, 30*time.Second)

	// Capture storage prefixes before pause.
	prefixesBeforePause := getStoragePrefixes(jobID)
	require.NotEmpty(t, prefixesBeforePause, "expected storage prefixes to be recorded before pause")
	t.Logf("storage prefixes before pause: %v", prefixesBeforePause)

	ensureJobState := func(targetState string) {
		testutils.SucceedsWithin(t, func() error {
			var status string
			tdb.QueryRow(t, `SELECT status FROM [SHOW JOB $1]`, jobID).Scan(&status)
			if status != targetState {
				return errors.Errorf("expected %s, got %s", targetState, status)
			}
			return nil
		}, 30*time.Second)
	}

	// Pause the job.
	tdb.Exec(t, `PAUSE JOB $1`, jobID)
	ensureJobState("paused")

	// Unblock the iteration hook so it can exit cleanly.
	close(iterationCh)

	// Resume the job.
	tdb.Exec(t, `RESUME JOB $1`, jobID)
	ensureJobState("succeeded")

	require.NoError(t, g.Wait())

	// Verify storage prefixes are preserved after resume.
	prefixesAfterResume := getStoragePrefixes(jobID)
	require.NotEmpty(t, prefixesAfterResume, "expected storage prefixes to exist after resume")
	t.Logf("storage prefixes after resume: %v", prefixesAfterResume)

	// The prefixes from before pause should still be present.
	for _, prefixBefore := range prefixesBeforePause {
		found := false
		for _, prefixAfter := range prefixesAfterResume {
			if prefixBefore == prefixAfter {
				found = true
				break
			}
		}
		require.True(t, found, "prefix %s from before pause should be preserved after resume", prefixBefore)
	}

	// Verify that temporary SST files were cleaned up after job completion.
	// Single-node cluster, so just check node 1's job directory.
	jobDir := fmt.Sprintf("%s/n1/job/%d", tempDir, jobID)
	_, err := os.Stat(jobDir)
	require.True(t, oserror.IsNotExist(err),
		"expected job directory %s to be cleaned up after job completion, but it still exists", jobDir)
	t.Logf("verified cleanup: job directory %s does not exist", jobDir)
}

func TestMultiMergeIndexBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create a temp directory for nodelocal storage used by distributed merge.
	tempDir := t.TempDir()

	// Track manifests from each iteration using the testing knob.
	manifestCountByIteration := make(map[int]int, 0)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: tempDir,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL: &execinfra.TestingKnobs{
				AfterDistributedMergeIteration: func(ctx context.Context, iteration int, manifests []jobspb.IndexBackfillSSTManifest) {
					manifestCountByIteration[iteration] = len(manifests)
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(db)

	tdb.Exec(t, `SET CLUSTER SETTING bulkio.index_backfill.distributed_merge.mode = 'declarative'`)
	tdb.Exec(t, `SET CLUSTER SETTING bulkio.index_backfill.distributed_merge.iterations = 4`)
	tdb.Exec(t, `SET CLUSTER SETTING bulkio.merge.file_size = '50KiB'`)

	tdb.Exec(t, `CREATE TABLE t (k INT PRIMARY KEY, v TEXT)`)
	tdb.Exec(t, `INSERT INTO t SELECT i, repeat('x', 100) || i::text FROM generate_series(1, 5000) AS g(i)`)

	// Trigger the distributed merge index backfill. It validates the final rows
	// itself, so a successful run implies the new pipeline produced correct
	// results. The remaining validation checks that intermediate files were
	// created and recorded in job progress for each iteration.
	tdb.Exec(t, `CREATE INDEX idx ON t (v)`)

	// Verify that we saw all intermediate iterations (1, 2, 3) via the testing knob.
	// Iteration 4 is the final one that ingests to KV, so it doesn't generate manifests.
	require.GreaterOrEqual(t, manifestCountByIteration[1], 12)
	require.GreaterOrEqual(t, manifestCountByIteration[2], 12)
	require.GreaterOrEqual(t, manifestCountByIteration[3], 12)
}

// pauseResumeTestState encapsulates shared state used by testing hooks
// during pause/resume tests. This state is reset for each subtest variant.
type pauseResumeTestState struct {
	backfillProgressCompletedCh chan []roachpb.Span
	isBlockingBackfillProgress  atomic.Bool
	manifestCountByIteration    map[int]int
	currentIteration            atomic.Int32
	finalIterationCompleted     atomic.Bool
	iterationContinueCh         chan struct{}
	mapPhaseContinueCh          chan struct{}
	currentTestName             string
	pauseDuringMapPhase         bool
	pauseAfterMapPhase          bool
	pauseAfterIteration         int
	mapPhaseManifestCount       atomic.Int32
}

// reset initializes the state for a new subtest run.
func (s *pauseResumeTestState) reset(
	testName string, pauseDuringMap bool, pauseAfterMap bool, pauseAfterIter int,
) {
	s.currentTestName = testName
	s.pauseDuringMapPhase = pauseDuringMap
	s.pauseAfterMapPhase = pauseAfterMap
	s.pauseAfterIteration = pauseAfterIter
	s.backfillProgressCompletedCh = make(chan []roachpb.Span)
	s.manifestCountByIteration = make(map[int]int, 0)
	s.currentIteration.Store(0)
	s.finalIterationCompleted.Store(false)
	s.mapPhaseManifestCount.Store(0)
	s.iterationContinueCh = make(chan struct{})
	s.mapPhaseContinueCh = make(chan struct{})
	s.isBlockingBackfillProgress.Store(true)
}

// waitForCheckpointPersisted waits for checkpoint data to be persisted to the job payload.
// For map phase pauses, it verifies that expectedSpans are checkpointed.
// For iteration pauses, it verifies that SST manifests are checkpointed.
func waitForCheckpointPersisted(
	t *testing.T, ctx context.Context, db *gosql.DB, jobID int, expectedSpans []roachpb.Span,
) {
	testutils.SucceedsWithin(t, func() error {
		stmt := `SELECT payload FROM crdb_internal.system_jobs WHERE id = $1`
		var payloadBytes []byte
		if err := db.QueryRowContext(ctx, stmt, jobID).Scan(&payloadBytes); err != nil {
			return err
		}

		payload := &jobspb.Payload{}
		if err := protoutil.Unmarshal(payloadBytes, payload); err != nil {
			return err
		}

		schemaChangeDetails := payload.Details.(*jobspb.Payload_NewSchemaChange).NewSchemaChange
		if len(schemaChangeDetails.BackfillProgress) == 0 {
			return errors.Errorf("no backfill progress found")
		}

		// If expectedSpans is provided, verify completed spans are checkpointed.
		if len(expectedSpans) > 0 {
			checkpointedSpans := schemaChangeDetails.BackfillProgress[0].CompletedSpans
			var checkpointedGroup roachpb.SpanGroup
			checkpointedGroup.Add(checkpointedSpans...)

			var expectedGroup roachpb.SpanGroup
			expectedGroup.Add(expectedSpans...)

			if !checkpointedGroup.Encloses(expectedGroup.Slice()...) {
				return errors.Errorf("checkpoint doesn't contain all observed spans yet")
			}
			return nil
		}

		// Otherwise, verify SST manifests are checkpointed (iteration pause case).
		ssts := schemaChangeDetails.BackfillProgress[0].SSTManifests
		if len(ssts) == 0 {
			return errors.Errorf("no SST manifests checkpointed yet")
		}
		t.Logf("checkpoint contains %d SST manifests", len(ssts))
		return nil
	}, 5*time.Second)
}

// TestDistributedMergeResumePreservesProgress tests that spans completed during
// a distributed merge backfill are properly preserved during PAUSE/RESUMEs.
func TestDistributedMergeResumePreservesProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "slow timing sensitive test")

	ctx := context.Background()
	const numRows = 500
	const numSpans = 10

	// Shared state for testing hooks - reset for each subtest.
	var state pauseResumeTestState
	var codec keys.SQLCodec

	// Create temp directory and server once for all subtests.
	tempDir := t.TempDir()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: tempDir,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL: &execinfra.TestingKnobs{
				BulkAdderFlushesEveryBatch: true,
				RunBeforeIndexBackfillProgressUpdate: func(ctx context.Context, completed []roachpb.Span) {
					if state.isBlockingBackfillProgress.Load() && state.pauseDuringMapPhase {
						if toRemove := len(codec.TenantPrefix()); toRemove > 0 {
							updated := make([]roachpb.Span, len(completed))
							for i := range updated {
								sp := completed[i]
								updated[i] = roachpb.Span{
									Key:    append(roachpb.Key(nil), sp.Key[toRemove:]...),
									EndKey: append(roachpb.Key(nil), sp.EndKey[toRemove:]...),
								}
							}
							completed = updated
						}
						select {
						case <-ctx.Done():
						case state.backfillProgressCompletedCh <- completed:
							t.Logf("[%s] before index backfill progress update, completed spans: %v", state.currentTestName, completed)
						}
					}
				},
				AfterDistributedMergeMapPhase: func(ctx context.Context, manifests []jobspb.IndexBackfillSSTManifest) {
					state.mapPhaseManifestCount.Store(int32(len(manifests)))
					t.Logf("[%s] after distributed merge map phase, manifests count: %d", state.currentTestName, len(manifests))

					if state.pauseAfterMapPhase {
						t.Logf("[%s] map phase complete, blocking to allow test to pause job", state.currentTestName)
						select {
						case <-state.mapPhaseContinueCh:
							t.Logf("[%s] unblocked by test, continuing with merge iterations", state.currentTestName)
						case <-ctx.Done():
							t.Logf("[%s] context cancelled (job paused), exiting hook", state.currentTestName)
							return
						}
					}
				},
				AfterDistributedMergeIteration: func(ctx context.Context, iteration int, manifests []jobspb.IndexBackfillSSTManifest) {
					state.currentIteration.Store(int32(iteration))
					state.manifestCountByIteration[iteration] = len(manifests)
					t.Logf("[%s] after distributed merge iteration %d, manifests count: %d", state.currentTestName, iteration, len(manifests))

					if len(manifests) == 0 {
						t.Logf("[%s] final iteration %d completed (direct KV ingest)", state.currentTestName, iteration)
						state.finalIterationCompleted.Store(true)
					}

					// Only block if this is NOT the final iteration (len(manifests) > 0).
					// On resume, the final iteration will have len(manifests)==0, and we don't want to block.
					if state.pauseAfterIteration > 0 && iteration == state.pauseAfterIteration && len(manifests) > 0 {
						t.Logf("[%s] iteration %d complete, blocking to allow test to pause job", state.currentTestName, iteration)
						select {
						case <-state.iterationContinueCh:
							t.Logf("[%s] unblocked by test, continuing with remaining iterations", state.currentTestName)
						case <-ctx.Done():
							t.Logf("[%s] context cancelled (job paused), exiting hook", state.currentTestName)
							return
						}
					}
				},
			},
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
				RunBeforeBackfill: func(progresses []scexec.BackfillProgress) error {
					if progresses != nil && state.pauseDuringMapPhase {
						t.Logf("[%s] before resuming backfill, checkpointed spans: %v", state.currentTestName, progresses[0].CompletedSpans)
					}
					return nil
				},
				AfterStage: func(p scplan.Plan, stageIdx int) error {
					if p.Stages[stageIdx].Type() != scop.BackfillType || !state.isBlockingBackfillProgress.Load() {
						return nil
					}
					if state.pauseDuringMapPhase {
						state.isBlockingBackfillProgress.Store(false)
						close(state.backfillProgressCompletedCh)
					}
					return nil
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	codec = s.Codec()

	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.index_backfill.distributed_merge.mode = 'declarative'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.index_backfill.distributed_merge.iterations = 4`)
	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.merge.file_size = '50KiB'`)

	// Create a table and splits that we can use for all subtests.
	sqlDB.Exec(t, `CREATE TABLE t(i INT PRIMARY KEY, v TEXT)`)
	sqlDB.Exec(t, `INSERT INTO t SELECT i, repeat('x', 100) || i::text FROM generate_series(1, $1) AS g(i)`, (numRows*numSpans)+1)
	for split := 0; split < numSpans; split++ {
		sqlDB.Exec(t, `ALTER TABLE t SPLIT AT VALUES ($1)`, numRows*split)
	}

	testIdx := 0
	for _, tc := range []struct {
		name                string
		pauseDuringMapPhase bool
		pauseAfterMapPhase  bool
		pauseAfterIteration int // 0 means don't pause after iteration, >0 means pause after that iteration
		checkpointInterval  string
		waitForCheckpoint   bool
	}{
		{
			name:                "fast checkpoint pause during map",
			pauseDuringMapPhase: true,
			checkpointInterval:  "10ms",
			waitForCheckpoint:   true,
		},
		{
			name:                "slow checkpoint pause during map",
			pauseDuringMapPhase: true,
			checkpointInterval:  "5s",
			waitForCheckpoint:   false,
		},
		{
			name:               "pause after map phase before merge",
			pauseAfterMapPhase: true,
			checkpointInterval: "10ms",
			waitForCheckpoint:  true,
		},
		{
			name:                "pause after iteration 1 with checkpoint",
			pauseAfterIteration: 1,
			checkpointInterval:  "10ms",
			waitForCheckpoint:   true,
		},
		{
			name:                "pause after iteration 2 with checkpoint",
			pauseAfterIteration: 2,
			checkpointInterval:  "10ms",
			waitForCheckpoint:   true,
		},
		{
			name:                "pause after iteration 3 with checkpoint",
			pauseAfterIteration: 3,
			checkpointInterval:  "10ms",
			waitForCheckpoint:   true,
		},
	} {
		// Use unique index name per subtest to avoid job ID confusion.
		testIdx++
		indexName := fmt.Sprintf("idx%d", testIdx)

		t.Run(tc.name, func(t *testing.T) {
			state.reset(tc.name, tc.pauseDuringMapPhase, tc.pauseAfterMapPhase, tc.pauseAfterIteration)

			// Set checkpoint interval based on test variant.
			sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.index_backfill.checkpoint_interval = $1`, tc.checkpointInterval)
			t.Logf("checkpoint interval set to %s, waitForCheckpoint=%v", tc.checkpointInterval, tc.waitForCheckpoint)

			var jobID int
			g := ctxgroup.WithContext(ctx)
			g.GoCtx(func(ctx context.Context) error {
				_, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX %s ON t (v)`, indexName))
				if err != nil && err.Error() != fmt.Sprintf("pq: job %d was paused before it completed", jobID) {
					return err
				}
				return nil
			})

			testutils.SucceedsWithin(t, func() error {
				jobIDRow := db.QueryRow(`
					SELECT job_id FROM [SHOW JOBS]
					WHERE job_type = 'NEW SCHEMA CHANGE' AND description ILIKE $1
					ORDER BY created DESC LIMIT 1`,
					fmt.Sprintf("%%CREATE INDEX %s%%", indexName),
				)
				if err := jobIDRow.Scan(&jobID); err != nil {
					return err
				}
				return nil
			}, 30*time.Second)

			ensureJobState := func(targetState string) {
				testutils.SucceedsWithin(t, func() error {
					var jobState string
					statusRow := db.QueryRow(`SELECT status FROM [SHOW JOB $1]`, jobID)
					if err := statusRow.Scan(&jobState); err != nil {
						return err
					}
					if jobState != targetState {
						return errors.Errorf("expected job to be %s, but found status: %s",
							targetState, jobState)
					}
					return nil
				}, 30*time.Second)
			}

			pauseAndResumeJob := func() {
				sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
				ensureJobState("paused")
				sqlDB.Exec(t, `RESUME JOB $1`, jobID)
				ensureJobState("running")
			}

			var observedSpansBeforePause []roachpb.Span
			var observedSpansAfterResume []roachpb.Span

			if tc.pauseDuringMapPhase {
				// Wait for some progress updates to accumulate.
				for i := 0; i < 3; i++ {
					progressUpdate := <-state.backfillProgressCompletedCh
					observedSpansBeforePause = append(observedSpansBeforePause, progressUpdate...)
				}

				if tc.waitForCheckpoint {
					waitForCheckpointPersisted(t, ctx, db, jobID, observedSpansBeforePause)
				}

				pauseAndResumeJob()
				ensureJobState("running")

				// Collect all remaining progress updates after resume.
				for state.isBlockingBackfillProgress.Load() {
					progressUpdate := <-state.backfillProgressCompletedCh
					observedSpansAfterResume = append(observedSpansAfterResume, progressUpdate...)
				}

				if tc.waitForCheckpoint {
					// Verify no span was processed both before and after pause.
					for _, afterSpan := range observedSpansAfterResume {
						for _, beforeSpan := range observedSpansBeforePause {
							if afterSpan.Equal(beforeSpan) {
								t.Fatalf("duplicate work: span %s processed before pause and again after resume", afterSpan)
							}
						}
					}
				}

			} else if tc.pauseAfterMapPhase {
				// Wait for the map phase to complete (hook will block).
				testutils.SucceedsWithin(t, func() error {
					if state.mapPhaseManifestCount.Load() > 0 {
						return nil
					}
					return errors.Errorf("waiting for map phase to complete")
				}, 30*time.Second)

				if tc.waitForCheckpoint {
					waitForCheckpointPersisted(t, ctx, db, jobID, nil)
				}

				// Pause the job while it's blocked in the AfterDistributedMergeMapPhase hook.
				sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
				ensureJobState("paused")

				// Unblock the hook so it can exit cleanly.
				close(state.mapPhaseContinueCh)

				// Resume the job.
				sqlDB.Exec(t, `RESUME JOB $1`, jobID)
				ensureJobState("running")

			} else if tc.pauseAfterIteration > 0 {
				// Wait for the specified iteration to complete.
				testutils.SucceedsWithin(t, func() error {
					if state.currentIteration.Load() >= int32(tc.pauseAfterIteration) {
						return nil
					}
					return errors.Errorf("waiting for iteration %d, current: %d",
						tc.pauseAfterIteration, state.currentIteration.Load())
				}, 30*time.Second)

				if tc.waitForCheckpoint {
					waitForCheckpointPersisted(t, ctx, db, jobID, nil)
				}

				pauseAndResumeJob()
			}

			// Now we can wait for the job to succeed.
			ensureJobState("succeeded")
			require.NoError(t, g.Wait())

			// Verify the final index is correct by checking row count.
			var indexRowCount int
			sqlDB.QueryRow(t, fmt.Sprintf(`SELECT count(*) FROM t@%s`, indexName)).Scan(&indexRowCount)
			require.Equal(t, (numRows*numSpans)+1, indexRowCount, "index should contain all rows")

			if tc.pauseAfterMapPhase {
				// Verify that merge iterations ran after resume.
				require.True(t, state.finalIterationCompleted.Load(),
					"expected final iteration (KV ingest) to complete after pause/resume from map phase")
				require.Greater(t, state.mapPhaseManifestCount.Load(), int32(0),
					"expected manifests to be produced during map phase")
				t.Logf("resumption from map phase completed with %d manifests, final iteration: %d",
					state.mapPhaseManifestCount.Load(), state.currentIteration.Load())
			}

			if tc.pauseAfterIteration > 0 {
				// Verify that resumption runs a single merge iteration directly to KV.
				// When checkpoint is persisted, the system detects DistributedMergePhase >= 1
				// and runs one final merge iteration to KV rather than restarting the pipeline.
				require.True(t, state.finalIterationCompleted.Load(),
					"expected final iteration (KV ingest) to complete after pause/resume at iteration %d",
					tc.pauseAfterIteration)
				t.Logf("resumption from iteration %d completed with final KV ingest (total iterations: %d)",
					tc.pauseAfterIteration, state.currentIteration.Load())
			}

			t.Logf("manifest counts by iteration: %v", state.manifestCountByIteration)
		})
	}
}
