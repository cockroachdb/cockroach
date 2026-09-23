package cspann_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/memstore"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func TestSearchOptionsDisableSplitMergeFixupsSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		disable bool
		want    int
	}{
		{name: "enabled", disable: false, want: 1},
		{name: "disabled", disable: true, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			q := quantize.NewUnQuantizer(2, vecpb.L2SquaredDistance)
			store := memstore.New(q, 42)
			opts := cspann.IndexOptions{
				RotAlgorithm:          vecpb.RotNone,
				IsDeterministic:       true,
				DisableAdaptiveSearch: true,
				MinPartitionSize:      1,
				MaxPartitionSize:      2,
				MaxWorkers:            1,
			}
			idx, err := cspann.NewIndex(ctx, store, q, 42, &opts, stopper)
			require.NoError(t, err)

			treeKey := memstore.ToTreeKey(1)
			rootMD := cspann.MakeReadyPartitionMetadata(cspann.LeafLevel, make(vector.T, q.GetDims()))
			require.NoError(t, store.TryCreateEmptyPartition(ctx, treeKey, cspann.RootKey, rootMD))

			require.NoError(t, store.RunTransaction(ctx, func(txn cspann.Txn) error {
				if err := txn.AddToPartition(ctx, treeKey, cspann.RootKey, cspann.LeafLevel, vector.T{0, 0},
					cspann.ChildKey{KeyBytes: cspann.KeyBytes("a")}, nil); err != nil {
					return err
				}
				if err := txn.AddToPartition(ctx, treeKey, cspann.RootKey, cspann.LeafLevel, vector.T{1, 1},
					cspann.ChildKey{KeyBytes: cspann.KeyBytes("b")}, nil); err != nil {
					return err
				}
				if err := txn.AddToPartition(ctx, treeKey, cspann.RootKey, cspann.LeafLevel, vector.T{2, 2},
					cspann.ChildKey{KeyBytes: cspann.KeyBytes("c")}, nil); err != nil {
					return err
				}
				return nil
			}))
			require.Equal(t, 0, idx.Fixups().PendingSplitsMerges())

			require.NoError(t, store.RunTransaction(ctx, func(txn cspann.Txn) error {
				var idxCtx cspann.Context
				idxCtx.Init(txn)

				searchSet := cspann.SearchSet{MaxResults: 1}
				return idx.Search(ctx, &idxCtx, treeKey, vector.T{0, 0}, &searchSet, cspann.SearchOptions{
					SkipRerank:              true,
					DisableSplitMergeFixups: tc.disable,
				})
			}))

			require.Equal(t, tc.want, idx.Fixups().PendingSplitsMerges())
		})
	}
}

func TestSearchOptionsDisableSplitMergeFixupsMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		disable bool
		want    int
	}{
		{name: "enabled", disable: false, want: 1},
		{name: "disabled", disable: true, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			q := quantize.NewUnQuantizer(2, vecpb.L2SquaredDistance)
			store := memstore.New(q, 42)
			opts := cspann.IndexOptions{
				RotAlgorithm:          vecpb.RotNone,
				IsDeterministic:       true,
				DisableAdaptiveSearch: true,
				MinPartitionSize:      2,
				MaxPartitionSize:      128,
				BaseBeamSize:          2,
				MaxWorkers:            1,
			}
			idx, err := cspann.NewIndex(ctx, store, q, 42, &opts, stopper)
			require.NoError(t, err)

			treeKey := memstore.ToTreeKey(1)

			leaf1 := store.MakePartitionKey()
			leaf2 := store.MakePartitionKey()

			leaf1MD := cspann.MakeReadyPartitionMetadata(cspann.LeafLevel, vector.T{0, 0})
			leaf2MD := cspann.MakeReadyPartitionMetadata(cspann.LeafLevel, vector.T{10, 10})
			require.NoError(t, store.TryCreateEmptyPartition(ctx, treeKey, leaf1, leaf1MD))
			require.NoError(t, store.TryCreateEmptyPartition(ctx, treeKey, leaf2, leaf2MD))

			rootMD := cspann.MakeReadyPartitionMetadata(cspann.SecondLevel, make(vector.T, q.GetDims()))
			require.NoError(t, store.TryCreateEmptyPartition(ctx, treeKey, cspann.RootKey, rootMD))

			require.NoError(t, store.RunTransaction(ctx, func(txn cspann.Txn) error {
				if err := txn.AddToPartition(ctx, treeKey, cspann.RootKey, cspann.SecondLevel, leaf1MD.Centroid,
					cspann.ChildKey{PartitionKey: leaf1}, nil); err != nil {
					return err
				}
				if err := txn.AddToPartition(ctx, treeKey, cspann.RootKey, cspann.SecondLevel, leaf2MD.Centroid,
					cspann.ChildKey{PartitionKey: leaf2}, nil); err != nil {
					return err
				}

				// leaf1 is under-sized, leaf2 is not.
				if err := txn.AddToPartition(ctx, treeKey, leaf1, cspann.LeafLevel, vector.T{0, 0},
					cspann.ChildKey{KeyBytes: cspann.KeyBytes("a")}, nil); err != nil {
					return err
				}
				if err := txn.AddToPartition(ctx, treeKey, leaf2, cspann.LeafLevel, vector.T{10, 10},
					cspann.ChildKey{KeyBytes: cspann.KeyBytes("b")}, nil); err != nil {
					return err
				}
				if err := txn.AddToPartition(ctx, treeKey, leaf2, cspann.LeafLevel, vector.T{11, 11},
					cspann.ChildKey{KeyBytes: cspann.KeyBytes("c")}, nil); err != nil {
					return err
				}
				return nil
			}))
			require.Equal(t, 0, idx.Fixups().PendingSplitsMerges())

			require.NoError(t, store.RunTransaction(ctx, func(txn cspann.Txn) error {
				var idxCtx cspann.Context
				idxCtx.Init(txn)

				searchSet := cspann.SearchSet{MaxResults: 1}
				return idx.Search(ctx, &idxCtx, treeKey, vector.T{0, 0}, &searchSet, cspann.SearchOptions{
					BaseBeamSize:            2,
					SkipRerank:              true,
					DisableSplitMergeFixups: tc.disable,
				})
			}))

			require.Equal(t, tc.want, idx.Fixups().PendingSplitsMerges())
		})
	}
}
