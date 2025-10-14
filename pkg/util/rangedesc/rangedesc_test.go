// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangedesc_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

var splits = [][]roachpb.Key{
	{},                                    // no splits
	{keys.Meta2Prefix},                    // split between meta1 and meta2
	{keys.SystemPrefix},                   // split after the meta range
	{keys.Meta2Prefix, keys.SystemPrefix}, // split before and after meta2
	{keys.RangeMetaKey(roachpb.RKey("middle")).AsRawKey()},                   // split within meta2
	{keys.Meta2Prefix, keys.RangeMetaKey(roachpb.RKey("middle")).AsRawKey()}, // split at start of and within meta2
}

var scopes = []roachpb.Span{
	keys.EverythingSpan,   // = /M{in-ax}
	keys.NodeLivenessSpan, // = /System/NodeLiveness{-Max}
	keys.TimeseriesSpan,   // = /System{/tsd-tse}
	keys.Meta1Span,        // = /M{in-eta2/}
	{ // = /{Meta1/-System}
		Key:    keys.MetaMin,
		EndKey: keys.MetaMax,
	},
	{ // = /Table/{3-6}
		Key:    keys.SystemDescriptorTableSpan.Key,
		EndKey: keys.SystemZonesTableSpan.EndKey,
	},
	{ // = /Table/{38-48}
		Key:    keys.SystemSQLCodec.TablePrefix(keys.TenantsRangesID),
		EndKey: keys.SystemSQLCodec.TablePrefix(keys.SpanConfigurationsTableID + 1),
	},
	{ // = /Table/{0-Max}
		Key:    keys.TableDataMin,
		EndKey: keys.TableDataMax,
	},
}

func TestEverythingScanner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	for _, s := range splits {
		t.Run(fmt.Sprintf("with-splits-at=%s", s), func(t *testing.T) {
			server, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
				DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			})
			defer server.Stopper().Stop(context.Background())

			for _, split := range s {
				if _, _, err := server.SplitRange(split); err != nil {
					t.Fatal(err)
				}
			}

			var numRanges int
			if err := server.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				numRanges = s.ReplicaCount()
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			scanner := rangedesc.NewScanner(kvDB)
			for _, pageSize := range []int{1, 5, 10, 50} {
				var numDescs int
				init := func() { numDescs = 0 }
				if err := scanner.Scan(ctx, pageSize, init, keys.EverythingSpan,
					func(descriptors ...roachpb.RangeDescriptor) error {
						numDescs += len(descriptors)
						return nil
					}); err != nil {
					t.Fatal(err)
				}

				if numDescs != numRanges {
					t.Fatalf("expected to find %d ranges, found %d", numRanges, numDescs)
				}
			}
		})
	}
}

// TestDataDriven is a data-driven test for rangedesc. The following syntax
// is provided:
//
//   - "iter" [page-size=<int>] [scope=<int>]
//   - "split" [set=<int>]
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		ctx := context.Background()
		server, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		})
		defer server.Stopper().Stop(context.Background())

		scanner := rangedesc.NewScanner(kvDB)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var buf strings.Builder

			switch d.Cmd {
			case "scan":
				pageSize := 1
				if d.HasArg("page-size") {
					d.ScanArgs(t, "page-size", &pageSize)
				}
				var scopeIdx int
				if d.HasArg("scope") {
					d.ScanArgs(t, "scope", &scopeIdx)
					require.True(t, scopeIdx >= 0 && scopeIdx < len(scopes))
				}
				scope := scopes[scopeIdx]

				var numDescs int
				init := func() { numDescs = 0 }
				if err := scanner.Scan(ctx, pageSize, init, scope,
					func(descriptors ...roachpb.RangeDescriptor) error {
						for _, desc := range descriptors {
							buf.WriteString(fmt.Sprintf("- r%d:%s\n", desc.RangeID, desc.KeySpan().String()))
						}
						numDescs += len(descriptors)
						return nil
					}); err != nil {
					t.Fatal(err)
				}

				var numRanges int
				if err := server.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
					numRanges = s.ReplicaCount()
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				buf.WriteString(fmt.Sprintf(
					"scan through %s (page-size=%d) found %d/%d descriptors\n",
					scope, pageSize, numDescs, numRanges))

			case "split":
				var set int
				d.ScanArgs(t, "set", &set)
				require.True(t, set >= 0 && set < len(splits))
				for _, split := range splits[set] {
					buf.WriteString(fmt.Sprintf("splitting at %s\n", split))
					if _, _, err := server.SplitRange(split); err != nil {
						t.Fatal(err)
					}
				}

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}

			return buf.String()
		})
	})
}

func TestIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	for _, s := range splits {
		t.Run(fmt.Sprintf("with-splits-at=%s", s), func(t *testing.T) {
			server, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
				DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			})
			defer server.Stopper().Stop(context.Background())

			for _, split := range s {
				_, _, err := server.SplitRange(split)
				require.NoError(t, err)
			}
			var numRanges int
			err := server.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				numRanges = s.ReplicaCount()
				return nil
			})
			require.NoError(t, err)

			iteratorFactory := rangedesc.NewIteratorFactory(kvDB)

			iter, err := iteratorFactory.NewIterator(ctx, keys.EverythingSpan)
			require.NoError(t, err)

			lazy, err := iteratorFactory.NewLazyIterator(ctx, keys.EverythingSpan, 2)
			require.NoError(t, err)

			var descs []roachpb.RangeDescriptor
			for iter.Valid() {
				require.True(t, lazy.Valid())
				require.Equal(t, iter.CurRangeDescriptor(), lazy.CurRangeDescriptor())
				descs = append(descs, iter.CurRangeDescriptor())
				iter.Next()
				lazy.Next()
			}
			if len(descs) != numRanges {
				t.Fatalf("expected to find %d ranges, found %d", numRanges, len(descs))
			}
		})
	}
}

// TestSplitMeta2AndPrintRanges splits meta2 range and prints all range descriptors.
func TestSplitMeta2AndPrintRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer server.Stopper().Stop(ctx)

	// fmt.Println("\n=== Initial state (no splits) ===")
	// printAllRanges(t, ctx, kvDB)

	//// Split at Meta2Prefix to separate meta1 and meta2
	//fmt.Println("\n=== Splitting at Meta2Prefix (separating meta1 and meta2) ===")
	//_, _, err := server.SplitRange(keys.Meta2Prefix)
	//require.NoError(t, err)
	printAllRanges(t, ctx, kvDB)

	// Iterate over meta range keys from MetaMin to MetaMax
	// This is similar to how it's done in pkg/kv/kvserver/loqrecovery/server.go
	fmt.Println("\n=== Iterating over meta ranges from MetaMin to MetaMax ===")
	const rangeMetadataScanChunkSize = 1000
	start := keys.Meta2Prefix
	rangeNum := 0

	for {
		kvs, err := kvDB.Scan(ctx, start, keys.MetaMax, rangeMetadataScanChunkSize)
		require.NoError(t, err)

		if len(kvs) == 0 {
			break
		}

		var endKey roachpb.Key
		for _, rangeDescKV := range kvs {
			endKey = rangeDescKV.Key
			rangeNum++

			var desc roachpb.RangeDescriptor
			if err := rangeDescKV.ValueProto(&desc); err != nil {
				fmt.Printf("  [%d] Key: %s - Error decoding: %v\n", rangeNum, rangeDescKV.Key, err)
				continue
			}

			fmt.Printf("\n[%d] Meta Key: %s\n", rangeNum, rangeDescKV.Key)
			fmt.Printf("    Range r%d:\n", desc.RangeID)
			fmt.Printf("      StartKey: %s\n", desc.StartKey)
			fmt.Printf("      EndKey:   %s\n", desc.EndKey)
			fmt.Printf("      Replicas: %v\n", desc.Replicas())
			fmt.Printf("      Generation: %d\n", desc.Generation)
		}

		// Move to the next key after the last one we scanned
		start = endKey.Next()
	}

	fmt.Printf("\nTotal meta range entries scanned: %d\n", rangeNum)
}

// printAllRanges prints all range descriptors in the cluster.
func printAllRanges(t *testing.T, ctx context.Context, kvDB *kv.DB) {
	scanner := rangedesc.NewScanner(kvDB)
	var rangeNum int
	err := scanner.Scan(ctx, 100, func() { rangeNum = 0 }, keys.EverythingSpan,
		func(descriptors ...roachpb.RangeDescriptor) error {
			for _, desc := range descriptors {
				rangeNum++
				fmt.Printf("Range %d: r%d [%s, %s)\n",
					rangeNum,
					desc.RangeID,
					desc.StartKey,
					desc.EndKey)
			}
			return nil
		})
	require.NoError(t, err)
	fmt.Printf("Total ranges: %d\n", rangeNum)
}
