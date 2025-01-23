// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scexec_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

//go:generate mockgen -package scexec_test -destination=mocks_generated_test.go --self_package scexec . Catalog,Dependencies,Backfiller,Merger,BackfillerTracker,IndexSpanSplitter,PeriodicProgressFlusher

// TestExecBackfiller uses generated mocks to ensure that the exec logic for
// backfills and merges deals with state appropriately.
func TestExecBackfiller(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	setupTestDeps := func(
		t *testing.T, tdb *sqlutils.SQLRunner, descs nstree.Catalog,
	) (*gomock.Controller, *MockBackfillerTracker, *MockBackfiller, *MockMerger, *sctestdeps.TestState) {
		mc := gomock.NewController(t)
		bt := NewMockBackfillerTracker(mc)
		bf := NewMockBackfiller(mc)
		m := NewMockMerger(mc)
		deps := sctestdeps.NewTestDependencies(
			sctestdeps.WithDescriptors(descs),
			sctestdeps.WithNamespace(sctestdeps.ReadNamespaceFromDB(t, tdb).Catalog),
			sctestdeps.WithBackfillerTracker(bt),
			sctestdeps.WithBackfiller(bf),
			sctestdeps.WithMerger(m),
		)
		return mc, bt, bf, m, deps
	}

	addIndexMutation := func(
		t *testing.T, mut *tabledesc.Mutable, name string, id descpb.IndexID, isTempIndex bool, columns ...string,
	) catalog.Index {
		var dirs []catenumpb.IndexColumn_Direction
		var columnIDs, keySuffixColumnIDs []descpb.ColumnID
		var columnIDSet catalog.TableColSet
		for _, c := range columns {
			col, err := catalog.MustFindColumnByName(mut, c)
			require.NoError(t, err)
			dirs = append(dirs, catenumpb.IndexColumn_ASC)
			columnIDs = append(columnIDs, col.GetID())
			columnIDSet.Add(col.GetID())
		}
		primaryIdx := mut.GetPrimaryIndex()
		for i := 0; i < primaryIdx.NumKeyColumns(); i++ {
			if id := primaryIdx.GetKeyColumnID(i); !columnIDSet.Contains(id) {
				keySuffixColumnIDs = append(keySuffixColumnIDs, id)
			}
		}
		require.NoError(t, mut.AddIndexMutation(&descpb.IndexDescriptor{
			Name:                        name,
			ID:                          id,
			Version:                     descpb.LatestIndexDescriptorVersion,
			KeyColumnNames:              columns,
			KeyColumnDirections:         dirs,
			KeyColumnIDs:                columnIDs,
			KeySuffixColumnIDs:          keySuffixColumnIDs,
			Type:                        idxtype.FORWARD,
			CreatedExplicitly:           true,
			EncodingType:                catenumpb.SecondaryIndexEncoding,
			UseDeletePreservingEncoding: isTempIndex,
		}, descpb.DescriptorMutation_ADD, descpb.DescriptorMutation_BACKFILLING))
		idx, err := catalog.MustFindIndexByName(mut, name)
		require.NoError(t, err)
		return idx
	}

	findTableWithName := func(c nstree.Catalog, name string) (tab catalog.TableDescriptor) {
		_ = c.ForEachDescriptor(func(desc catalog.Descriptor) error {
			var ok bool
			tab, ok = desc.(catalog.TableDescriptor)
			if ok && tab.GetName() == name {
				return iterutil.StopIteration()
			}
			return nil
		})
		return tab
	}
	getTableDescriptor := func(ctx context.Context, t *testing.T, deps *sctestdeps.TestState, id descpb.ID) catalog.TableDescriptor {
		descs, err := deps.Catalog().MustReadImmutableDescriptors(ctx, id)
		require.NoError(t, err)
		tab, ok := descs[0].(catalog.TableDescriptor)
		require.True(t, ok)
		return tab
	}
	backfillIndexOp := func(id descpb.ID, sourceIndexID, destIndexID descpb.IndexID) *scop.BackfillIndex {
		return &scop.BackfillIndex{TableID: id, SourceIndexID: sourceIndexID, IndexID: destIndexID}
	}

	type testCase struct {
		name string
		f    func(t *testing.T, tdb *sqlutils.SQLRunner)
	}
	testCases := []testCase{
		{name: "simple backfill", f: func(t *testing.T, tdb *sqlutils.SQLRunner) {
			tdb.Exec(t, "create table foo (i INT PRIMARY KEY, j INT)")
			descs := sctestdeps.ReadDescriptorsFromDB(ctx, t, tdb)
			tab := findTableWithName(descs.Catalog, "foo")
			require.NotNil(t, tab)
			mut := tabledesc.NewBuilder(tab.TableDesc()).BuildExistingMutableTable()
			addIndexMutation(t, mut, "idx", 2, false /* isTempIndex */, "j")
			descs.UpsertDescriptor(mut)
			mc, bt, bf, _, deps := setupTestDeps(t, tdb, descs.Catalog)
			defer mc.Finish()
			read, err := deps.Catalog().MustReadImmutableDescriptors(ctx, mut.GetID())
			require.NoError(t, err)
			desc := read[0]

			backfill := scexec.Backfill{
				TableID:       tab.GetID(),
				SourceIndexID: 1,
				DestIndexIDs:  []descpb.IndexID{2},
			}
			progress := scexec.BackfillProgress{Backfill: backfill}
			bt.EXPECT().
				GetBackfillProgress(gomock.Any(), backfill).
				Return(progress, nil)
			scanned := scexec.BackfillProgress{
				Backfill:              backfill,
				MinimumWriteTimestamp: hlc.Timestamp{WallTime: 1},
			}
			bf.EXPECT().
				MaybePrepareDestIndexesForBackfill(gomock.Any(), progress, desc).
				Return(scanned, nil)
			setProgress := bt.EXPECT().
				SetBackfillProgress(gomock.Any(), scanned)
			flushAfterScan := bt.EXPECT().
				FlushCheckpoint(gomock.Any()).
				After(setProgress)
			backfillCall := bf.EXPECT().
				BackfillIndexes(gomock.Any(), scanned, bt, deps.TransactionalJobRegistry().CurrentJob(), desc).
				After(flushAfterScan)
			bt.EXPECT().
				FlushCheckpoint(gomock.Any()).
				After(backfillCall)
			bt.EXPECT().
				FlushFractionCompleted(gomock.Any()).
				After(backfillCall)

			require.NoError(t, scexec.ExecuteStage(ctx, deps, scop.PostCommitPhase, []scop.Op{
				&scop.BackfillIndex{
					TableID:       tab.GetID(),
					SourceIndexID: 1,
					IndexID:       2,
				}}))
		}},
		{
			name: "two tables, two indexes each, one needs a scan",
			f: func(t *testing.T, tdb *sqlutils.SQLRunner) {
				tdb.Exec(t, "create table foo (i INT PRIMARY KEY, j INT, k INT)")
				tdb.Exec(t, "create table bar (i INT PRIMARY KEY, j INT, k INT)")
				descs := sctestdeps.ReadDescriptorsFromDB(ctx, t, tdb)
				var fooID descpb.ID
				{
					tab := findTableWithName(descs.Catalog, "foo")
					require.NotNil(t, tab)
					fooID = tab.GetID()
					mut := tabledesc.NewBuilder(tab.TableDesc()).BuildExistingMutableTable()
					addIndexMutation(t, mut, "idx", 2, false /* isTempIndex */, "j")
					addIndexMutation(t, mut, "idx", 3, false /* isTempIndex */, "k", "j")
					descs.UpsertDescriptor(mut)
				}
				var barID descpb.ID
				{
					tab := findTableWithName(descs.Catalog, "bar")
					require.NotNil(t, tab)
					barID = tab.GetID()
					mut := tabledesc.NewBuilder(tab.TableDesc()).BuildExistingMutableTable()
					addIndexMutation(t, mut, "idx", 4, false /* isTempIndex */, "j")
					addIndexMutation(t, mut, "idx", 5, false /* isTempIndex */, "k", "j")
					descs.UpsertDescriptor(mut)
				}

				mc, bt, bf, _, deps := setupTestDeps(t, tdb, descs.Catalog)
				defer mc.Finish()
				foo := getTableDescriptor(ctx, t, deps, fooID)
				bar := getTableDescriptor(ctx, t, deps, barID)

				backfillFoo := scexec.Backfill{
					TableID:       fooID,
					SourceIndexID: 1,
					DestIndexIDs:  []descpb.IndexID{2, 3},
				}
				progressFoo := scexec.BackfillProgress{
					Backfill:              backfillFoo,
					MinimumWriteTimestamp: hlc.Timestamp{WallTime: 1},
				}
				bt.EXPECT().
					GetBackfillProgress(gomock.Any(), backfillFoo).
					Return(progressFoo, nil)

				backfillBar := scexec.Backfill{
					TableID:       barID,
					SourceIndexID: 1,
					DestIndexIDs:  []descpb.IndexID{4, 5},
				}
				progressBar := scexec.BackfillProgress{
					Backfill: backfillBar,
				}
				scannedBar := scexec.BackfillProgress{
					Backfill:              backfillBar,
					MinimumWriteTimestamp: hlc.Timestamp{WallTime: 1},
				}
				bt.EXPECT().
					GetBackfillProgress(gomock.Any(), backfillBar).
					Return(progressBar, nil)
				bf.EXPECT().
					MaybePrepareDestIndexesForBackfill(gomock.Any(), progressBar, bar).
					Return(scannedBar, nil)
				setProgress := bt.EXPECT().
					SetBackfillProgress(gomock.Any(), scannedBar)
				flushAfterScan := bt.EXPECT().
					FlushCheckpoint(gomock.Any()).
					After(setProgress)
				backfillBarCall := bf.EXPECT().
					BackfillIndexes(gomock.Any(), scannedBar, bt, deps.TransactionalJobRegistry().CurrentJob(), bar).
					After(flushAfterScan)
				backfillFooCall := bf.EXPECT().
					BackfillIndexes(gomock.Any(), progressFoo, bt, deps.TransactionalJobRegistry().CurrentJob(), foo).
					After(flushAfterScan)
				bt.EXPECT().
					FlushCheckpoint(gomock.Any()).
					After(backfillFooCall).
					After(backfillBarCall)
				bt.EXPECT().
					FlushFractionCompleted(gomock.Any()).
					After(backfillFooCall).
					After(backfillBarCall)
				ops := []scop.Op{
					backfillIndexOp(barID, 1, 5),
					backfillIndexOp(fooID, 1, 3),
					backfillIndexOp(barID, 1, 4),
					backfillIndexOp(fooID, 1, 2),
				}
				rand.Shuffle(len(ops), func(i, j int) { ops[i], ops[j] = ops[j], ops[i] })
				require.NoError(t, scexec.ExecuteStage(ctx, deps, scop.PostCommitPhase, ops))
			},
		},
		{name: "simple merge", f: func(t *testing.T, tdb *sqlutils.SQLRunner) {
			tdb.Exec(t, "create table foo (i INT PRIMARY KEY, j INT)")
			descs := sctestdeps.ReadDescriptorsFromDB(ctx, t, tdb)
			tab := findTableWithName(descs.Catalog, "foo")
			require.NotNil(t, tab)
			mut := tabledesc.NewBuilder(tab.TableDesc()).BuildExistingMutableTable()
			addIdx := addIndexMutation(t, mut, "idx", 2, false /* isTempIndex */, "j")
			tmpIdx := addIndexMutation(t, mut, "idx_temp", 3, true /* isTempIndex */, "j")
			for i := range mut.Mutations {
				m := &mut.Mutations[i]
				idx := m.GetIndex()
				if idx == nil {
					continue
				}
				switch idx.ID {
				case addIdx.GetID():
					m.State = descpb.DescriptorMutation_MERGING
				case tmpIdx.GetID():
					m.State = descpb.DescriptorMutation_WRITE_ONLY
				}
			}
			descs.UpsertDescriptor(mut)
			mc, bt, _, m, deps := setupTestDeps(t, tdb, descs.Catalog)
			defer mc.Finish()
			read, err := deps.Catalog().MustReadImmutableDescriptors(ctx, mut.GetID())
			require.NoError(t, err)
			desc := read[0]

			merge := scexec.Merge{
				TableID:        tab.GetID(),
				SourceIndexIDs: []descpb.IndexID{tmpIdx.GetID()},
				DestIndexIDs:   []descpb.IndexID{addIdx.GetID()},
			}
			progress := scexec.MergeProgress{Merge: merge}
			getProgress := bt.EXPECT().
				GetMergeProgress(gomock.Any(), merge).
				Return(progress, nil)
			mergeCall := m.EXPECT().
				MergeIndexes(gomock.Any(), deps.CurrentJob(), progress, bt, desc).
				After(getProgress)
			bt.EXPECT().
				FlushCheckpoint(gomock.Any()).
				After(mergeCall)
			bt.EXPECT().
				FlushFractionCompleted(gomock.Any()).
				After(mergeCall)

			require.NoError(t, scexec.ExecuteStage(ctx, deps, scop.PostCommitPhase, []scop.Op{
				&scop.MergeIndex{
					TableID:           tab.GetID(),
					TemporaryIndexID:  tmpIdx.GetID(),
					BackfilledIndexID: addIdx.GetID(),
				}}))
		}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			defer log.Scope(t).Close(t)

			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)

			testCase.f(t, sqlutils.MakeSQLRunner(db))
		})
	}

}
