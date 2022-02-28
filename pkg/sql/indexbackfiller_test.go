// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	gosql "database/sql"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/startupmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestIndexBackfiller tests the scenarios described in docs/tech-notes/index-backfill.md
func TestIndexBackfiller(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()

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
				// Wait until we get a signal to move to DELETE_AND_WRITE_ONLY.
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
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	tc := serverutils.StartNewTestCluster(t, 3,
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
	// tempIndex: DELETE_AND_WRITE_ONLY
	// newIndex   BACKFILLING
	moveToTWrite <- true
	execOrFail("INSERT INTO t.kv VALUES (2, 'b')") // new_index: nothing, temp_index: sees insert

	// Pick our scan timestamp.
	// tempIndex: DELETE_AND_WRITE_ONLY
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
					Version:      descpb.PrimaryIndexWithStoredColumnsVersion,
					KeyColumnNames: []string{
						mut.Columns[2].Name,
					},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{
						descpb.IndexDescriptor_ASC,
					},
					KeyColumnIDs: []descpb.ColumnID{
						mut.Columns[2].ID,
					},
					KeySuffixColumnIDs: []descpb.ColumnID{
						mut.Columns[0].ID,
					},
					Type:         descpb.IndexDescriptor_FORWARD,
					EncodingType: descpb.SecondaryIndexEncoding,
				}
				mut.NextIndexID++
				mut.NextConstraintID++
				require.NoError(t, mut.AddIndexMutation(ctx,
					&indexToBackfill, descpb.DescriptorMutation_ADD, settings,
				))
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
					PGAttributeNum: uint32(mut.NextColumnID),
				}
				mut.NextColumnID++
				mut.AddColumnMutation(&columnWithDefault, descpb.DescriptorMutation_ADD)
				// Cheat and jump right to DELETE_AND_WRITE_ONLY.
				mut.Mutations[len(mut.Mutations)-1].State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
				computedColumnNotInPrimaryIndex := descpb.ColumnDescriptor{
					Name:           "comp",
					ID:             mut.NextColumnID,
					Type:           types.Int,
					Nullable:       false,
					ComputeExpr:    proto.String("i + def"),
					Hidden:         false,
					PGAttributeNum: uint32(mut.NextColumnID),
				}
				mut.NextColumnID++
				mut.AddColumnMutation(&computedColumnNotInPrimaryIndex, descpb.DescriptorMutation_ADD)
				// Cheat and jump right to DELETE_AND_WRITE_ONLY.
				mut.Mutations[len(mut.Mutations)-1].State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY

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
					Version:      descpb.PrimaryIndexWithStoredColumnsVersion,
					KeyColumnNames: []string{
						mut.Columns[0].Name,
					},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{
						descpb.IndexDescriptor_ASC,
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
					Type:         descpb.IndexDescriptor_FORWARD,
					EncodingType: descpb.PrimaryIndexEncoding,
				}
				mut.NextIndexID++
				mut.NextConstraintID++
				require.NoError(t, mut.AddIndexMutation(ctx,
					&indexToBackfill, descpb.DescriptorMutation_ADD, settings,
				))
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
		ctx context.Context, t *testing.T, txn *kv.Txn, table *tabledesc.Mutable, indexID descpb.IndexID,
	) []tree.Datums {
		t.Helper()

		mm := mon.MakeStandaloneBudget(1 << 30)
		idx, err := table.FindIndexWithID(indexID)
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
		spans := []roachpb.Span{table.IndexSpan(keys.SystemSQLCodec, indexID)}
		const reverse = false
		var fetcherCols []descpb.ColumnID
		for _, col := range table.PublicColumns() {
			if colIDsNeeded.Contains(col.GetID()) {
				fetcherCols = append(fetcherCols, col.GetID())
			}
		}
		var alloc tree.DatumAlloc
		var spec descpb.IndexFetchSpec
		require.NoError(t, rowenc.InitIndexFetchSpec(
			&spec,
			keys.SystemSQLCodec,
			table,
			idx,
			fetcherCols,
		))
		var fetcher row.Fetcher
		require.NoError(t, fetcher.Init(
			ctx,
			reverse,
			descpb.ScanLockingStrength_FOR_NONE,
			descpb.ScanLockingWaitPolicy_BLOCK,
			0,
			&alloc,
			mm.Monitor(),
			&spec,
		))

		require.NoError(t, fetcher.StartScan(
			ctx, txn, spans, rowinfra.NoBytesLimit, 0, true, false, /* forceProductionBatchSize */
		))
		var rows []tree.Datums
		for {
			datums, err := fetcher.NextRowDecoded(ctx)
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
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
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
			},
		})
		defer tc.Stopper().Stop(ctx)
		defer close(blockChan)

		// Run the initial setupSQL.
		tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		tdb.Exec(t, test.setupSQL)

		// Fetch the descriptor ID for the relevant table.
		var tableID descpb.ID
		tdb.QueryRow(t, "SELECT ($1::REGCLASS)::INT", test.tableName).Scan(&tableID)

		// Run the testCase's setupDesc function to prepare an index backfill
		// mutation. Also, create an associated job and set it up to be blocked.
		s0 := tc.Server(0)
		lm := s0.LeaseManager().(*lease.Manager)
		settings := s0.ClusterSettings()
		execCfg := s0.ExecutorConfig().(sql.ExecutorConfig)
		jr := s0.JobRegistry().(*jobs.Registry)
		var j *jobs.Job
		var table catalog.TableDescriptor
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) (err error) {
			mut, err := descriptors.GetMutableTableByID(ctx, txn, tableID, tree.ObjectLookupFlags{})
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
				Username:      security.RootUserName(),
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
			return descriptors.WriteDesc(ctx, false /* kvTrace */, mut, txn)
		}))

		// Run the index backfill
		changer := sql.NewSchemaChangerForTesting(
			tableID, 1, execCfg.NodeID.SQLInstanceID(), s0.DB(), lm, jr, &execCfg, settings)
		changer.SetJob(j)
		spans := []roachpb.Span{table.IndexSpan(keys.SystemSQLCodec, test.indexToBackfill)}
		require.NoError(t, changer.TestingDistIndexBackfill(ctx, table.GetVersion(), spans,
			[]descpb.IndexID{test.indexToBackfill}, backfill.IndexMutationFilter))

		// Make the mutation complete, then read the index and validate that it
		// has the expected contents.
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			table, err := descriptors.GetMutableTableByID(ctx, txn, tableID, tree.ObjectLookupFlags{})
			if err != nil {
				return err
			}
			toComplete := len(table.Mutations)
			for i := 0; i < toComplete; i++ {
				mut := table.Mutations[i]
				require.NoError(t, table.MakeMutationComplete(mut))
			}
			table.Mutations = table.Mutations[toComplete:]
			datums := fetchIndex(ctx, t, txn, table, test.indexToBackfill)
			require.Equal(t, test.expectedContents, datumSliceToStrMatrix(datums))
			return nil
		}))
	}

	for _, test := range indexBackfillerTestCases {
		t.Run(test.name, func(t *testing.T) { run(t, test) })
	}
}
