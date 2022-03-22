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
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

//go:generate mockgen -package scexec_test -destination=mocks_generated_test.go --self_package scexec . Catalog,Dependencies,Backfiller,BackfillTracker,IndexSpanSplitter,PeriodicProgressFlusher

// TestExecBackfill uses generated mocks to ensure that the exec logic for
// backfills deals with state appropriately.
func TestExecBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	setupTestDeps := func(
		t *testing.T, tdb *sqlutils.SQLRunner, descs nstree.Catalog,
	) (*gomock.Controller, *MockBackfillTracker, *MockBackfiller, *sctestdeps.TestState) {
		mc := gomock.NewController(t)
		bt := NewMockBackfillTracker(mc)
		bf := NewMockBackfiller(mc)
		deps := sctestdeps.NewTestDependencies(
			sctestdeps.WithDescriptors(descs),
			sctestdeps.WithNamespace(sctestdeps.ReadNamespaceFromDB(t, tdb).Catalog),
			sctestdeps.WithBackfillTracker(bt),
			sctestdeps.WithBackfiller(bf),
		)
		return mc, bt, bf, deps
	}

	addIndexMutation := func(
		t *testing.T, mut *tabledesc.Mutable, name string, id descpb.IndexID, columns ...string,
	) catalog.Index {
		var dirs []descpb.IndexDescriptor_Direction
		var columnIDs, keySuffixColumnIDs []descpb.ColumnID
		var columnIDSet catalog.TableColSet
		for _, c := range columns {
			col, err := mut.FindColumnWithName(tree.Name(c))
			require.NoError(t, err)
			dirs = append(dirs, descpb.IndexDescriptor_ASC)
			columnIDs = append(columnIDs, col.GetID())
			columnIDSet.Add(col.GetID())
		}
		primaryIdx := mut.GetPrimaryIndex()
		for i := 0; i < primaryIdx.NumKeyColumns(); i++ {
			if id := primaryIdx.GetKeyColumnID(i); !columnIDSet.Contains(id) {
				keySuffixColumnIDs = append(keySuffixColumnIDs, id)
			}
		}
		require.NoError(t, mut.DeprecatedAddIndexMutation(&descpb.IndexDescriptor{
			Name:                name,
			ID:                  id,
			Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
			KeyColumnNames:      columns,
			KeyColumnDirections: dirs,
			KeyColumnIDs:        columnIDs,
			KeySuffixColumnIDs:  keySuffixColumnIDs,
			Type:                descpb.IndexDescriptor_FORWARD,
			CreatedExplicitly:   true,
			EncodingType:        descpb.SecondaryIndexEncoding,
		}, descpb.DescriptorMutation_ADD))
		idx, err := mut.FindIndexWithName(name)
		require.NoError(t, err)
		return idx
	}

	findTableWithName := func(c nstree.Catalog, name string) (tab catalog.TableDescriptor) {
		_ = c.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
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
		{name: "simple", f: func(t *testing.T, tdb *sqlutils.SQLRunner) {
			tdb.Exec(t, "create table foo (i INT PRIMARY KEY, j INT)")
			descs := sctestdeps.ReadDescriptorsFromDB(ctx, t, tdb)
			tab := findTableWithName(descs.Catalog, "foo")
			require.NotNil(t, tab)
			mut := tabledesc.NewBuilder(tab.TableDesc()).BuildExistingMutableTable()
			addIndexMutation(t, mut, "idx", 2, "j")
			descs.UpsertDescriptorEntry(mut)
			mc, bt, bf, deps := setupTestDeps(t, tdb, descs.Catalog)
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
				BackfillIndex(gomock.Any(), scanned, bt, desc).
				After(flushAfterScan)
			bt.EXPECT().
				FlushCheckpoint(gomock.Any()).
				After(backfillCall)
			bt.EXPECT().
				FlushFractionCompleted(gomock.Any()).
				After(backfillCall)

			require.NoError(t, scexec.ExecuteStage(ctx, deps, []scop.Op{
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
					addIndexMutation(t, mut, "idx", 2, "j")
					addIndexMutation(t, mut, "idx", 3, "k", "j")
					descs.UpsertDescriptorEntry(mut)
				}
				var barID descpb.ID
				{
					tab := findTableWithName(descs.Catalog, "bar")
					require.NotNil(t, tab)
					barID = tab.GetID()
					mut := tabledesc.NewBuilder(tab.TableDesc()).BuildExistingMutableTable()
					addIndexMutation(t, mut, "idx", 4, "j")
					addIndexMutation(t, mut, "idx", 5, "k", "j")
					descs.UpsertDescriptorEntry(mut)
				}

				mc, bt, bf, deps := setupTestDeps(t, tdb, descs.Catalog)
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
					BackfillIndex(gomock.Any(), scannedBar, bt, bar).
					After(flushAfterScan)
				backfillFooCall := bf.EXPECT().
					BackfillIndex(gomock.Any(), progressFoo, bt, foo).
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
				require.NoError(t, scexec.ExecuteStage(ctx, deps, ops))
			}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			defer log.Scope(t).Close(t)

			tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
			defer tc.Stopper().Stop(ctx)

			testCase.f(t, sqlutils.MakeSQLRunner(tc.ServerConn(0)))
		})
	}

}
