// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"math/rand"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/errors"
)

type indexSplitAndScatter struct {
	db           *kv.DB
	codec        keys.SQLCodec
	sv           *settings.Values
	rangeIter    rangedesc.IteratorFactory
	nodeDescs    kvclient.NodeDescStore
	statsCache   *stats.TableStatisticsCache
	testingKnobs *ExecutorTestingKnobs
}

var SplitAndScatterWithStats = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"schemachanger.backfiller.split_with_stats.enabled",
	"when enabled the index backfiller will generate split and "+
		"scatter points based table statistics",
	false,
)

// NewIndexSplitAndScatter creates a new scexec.IndexSpanSplitter implementation.
func NewIndexSplitAndScatter(execCfg *ExecutorConfig) scexec.IndexSpanSplitter {
	return &indexSplitAndScatter{
		db:           execCfg.DB,
		codec:        execCfg.Codec,
		sv:           &execCfg.Settings.SV,
		rangeIter:    execCfg.RangeDescIteratorFactory,
		nodeDescs:    execCfg.NodeDescs,
		statsCache:   execCfg.TableStatsCache,
		testingKnobs: &execCfg.TestingKnobs,
	}
}

func (is *indexSplitAndScatter) getSplitPointsWithStats(
	ctx context.Context, table catalog.TableDescriptor, indexToBackfill catalog.Index, nSplits int,
) ([][]byte, error) {
	// Split and scatter with statistics is disabled.
	if !SplitAndScatterWithStats.Get(is.sv) {
		return nil, nil
	}
	// Fetch the current statistics for this table.
	tableStats, err := is.statsCache.GetTableStats(ctx, table, nil)
	if err != nil {
		return nil, err
	}
	// Nothing can be done since no stats exist.
	if len(tableStats) == 0 {
		return nil, errors.New("no stats exist for this table")
	}
	// Gather the latest stats for each column.
	keyCols := indexToBackfill.CollectKeyColumnIDs()
	statsForColumns := make(map[descpb.ColumnID]*stats.TableStatistic)
	keyCols.ForEach(func(col descpb.ColumnID) {
		for _, stat := range tableStats {
			// Skip stats that:
			// 1) Do not contain this column.
			// 2) Consist of multiple columns.
			// 3) Have no histogram information.
			if stat.Histogram == nil || len(stat.ColumnIDs) != 1 || stat.ColumnIDs[0] != col {
				continue
			}
			statsForColumns[col] = stat
			break
		}
	})
	rowsPerRange := tableStats[0].RowCount / uint64(nSplits)
	// Helper function that will append split points, and if necessary, downsample
	// them if they get too big.
	var splitPoints [][]byte
	appendAndShrinkSplitPoint := func(existing [][]byte, add []byte) [][]byte {
		maxSplitPoints := nSplits * 2
		if len(existing) < maxSplitPoints {
			return append(existing, add)
		}
		// Otherwise, we can sample these split points.
		sort.Slice(existing, func(i, j int) bool {
			return bytes.Compare(existing[i], existing[j]) < 0
		})
		// Next get this down to capacity again by taking a uniform sample of the
		// existing split points.
		newSplitPoints := make([][]byte, 0, nSplits+1)
		step := float64(len(existing)) / float64(nSplits)
		for i := 0; i < nSplits; i++ {
			newSplitPoints = append(newSplitPoints, existing[int(float64(i)*step)])
		}
		newSplitPoints = append(newSplitPoints, add)
		return newSplitPoints
	}
	// The following code generates split points for an index by iterating through
	// each column of the index. For each column, it uses histogram statistics to
	// identify points where the data can be divided into chunks of a target size
	// (`rowsPerRange`).
	//
	// For the first column, it creates initial split points. For each subsequent
	// column, it expands on the previously generated split points. It does this by
	// appending the new column's split values to each of the existing split points from
	// prior columns. This causes us to iterate combinatorially over all possible split points,
	// so the `appendAndShrinkSplitPoint` function is used to downsample and keep the total number
	// of points controlled.

	// Note: Sadly, only the primary key or columns in indexes will have
	// detailed information that we can use. All other columns will have
	// limited splits.
	for colIdx := 0; colIdx < indexToBackfill.NumKeyColumns(); colIdx++ {
		lastSplitPoints := append([][]byte{}, splitPoints...)
		splitPoints = splitPoints[:0]
		keyColID := indexToBackfill.GetKeyColumnID(colIdx)
		// Look up the stats and skip if they are missing.
		stat, ok := statsForColumns[keyColID]
		if !ok {
			break
		}
		numInBucket := uint64(0)
		for bucketIdx, bucket := range stat.Histogram {
			numInBucket += uint64(bucket.NumRange) + uint64(bucket.NumEq)
			// If we have hit the target rows, then emit a split point. Or
			// if we are on the last bucket, we should always emit one.
			if numInBucket >= rowsPerRange || bucketIdx == len(stat.Histogram)-1 {
				var prevKeys [][]byte
				// For the first column, we are going to start fresh with the base index prefix.
				if colIdx == 0 {
					prevKeys = [][]byte{is.codec.IndexPrefix(uint32(table.GetID()), uint32(indexToBackfill.GetID()))}
				} else {
					// For later columns we are going to start with the previous sets of splits.
					prevKeys = lastSplitPoints
				}
				// We don't know where later columns fall, so we will encode these
				// against all the previous split points (sadly, this will have an exponential
				// cost). Our limit on the number of split points will resample these if they
				// become excessive.
				for _, prevKey := range prevKeys {
					// Copy the base value before appending the next part of the key.
					if colIdx > 0 {
						tempKey := make([]byte, len(prevKey), cap(prevKey))
						copy(tempKey, prevKey)
						prevKey = tempKey
					}
					newSplit, err := keyside.Encode(prevKey, bucket.UpperBound, encoding.Direction(indexToBackfill.GetKeyColumnDirection(colIdx)+1))
					if err != nil {
						return nil, err
					}
					splitPoints = appendAndShrinkSplitPoint(splitPoints, newSplit)
				}
				numInBucket = 0
				continue
			}
		}
		// Stop once enough partitions have been created. Or if no partitions exist,
		// then there is insufficient data for an educated guess. As we process later
		// columns, we end up creating all possible permutations of the previous split
		// points we selected, which means the statistical likelihood of a valid split
		// point getting selected only gets lower.
		if len(splitPoints) >= nSplits || len(splitPoints) == 0 {
			break
		}
	}
	// Always emit a split point at the start of the index span if
	// we generated any split points above
	if len(splitPoints) > 0 {
		splitPoints = append(splitPoints, is.codec.IndexPrefix(uint32(table.GetID()), uint32(indexToBackfill.GetID())))
		log.Infof(ctx, "generated %d split points from statistics for tableId=%d index=%d", len(splitPoints), table.GetID(), indexToBackfill.GetID())
	}
	return splitPoints, nil
}

// MaybeSplitIndexSpans implements the scexec.IndexSpanSplitter interface.
func (is *indexSplitAndScatter) MaybeSplitIndexSpans(
	ctx context.Context, table catalog.TableDescriptor, indexToBackfill catalog.Index,
) error {
	// We will always pre-split index spans if there is partitioning.
	err := is.MaybeSplitIndexSpansForPartitioning(ctx, table, indexToBackfill)
	if err != nil {
		return err
	}

	const backfillSplitExpiration = time.Hour
	tableID := table.GetID()
	preservedSplitsMultiple := int(PreservedSplitCountMultiple.Get(is.sv))
	nNodes := is.nodeDescs.GetNodeDescriptorCount()
	nSplits := preservedSplitsMultiple * nNodes
	var copySplitsFromIndexID descpb.IndexID
	for _, idx := range table.ActiveIndexes() {
		if idx.GetID() != indexToBackfill.GetID() &&
			idx.CollectKeyColumnIDs().Equals(indexToBackfill.CollectKeyColumnIDs()) {
			copySplitsFromIndexID = idx.GetID()
			break
		}
	}

	// Re-split the new set of indexes along the same split points as the old
	// indexes.
	b := is.db.NewBatch()
	tablePrefix := is.codec.TablePrefix(uint32(tableID))

	// Fetch all of the range descriptors for this index.
	rangeDescIterator, err := is.rangeIter.NewIterator(ctx, roachpb.Span{
		Key:    tablePrefix,
		EndKey: tablePrefix.PrefixEnd(),
	})
	if err != nil {
		return err
	}

	// Shift the range split points from the old keyspace into the new keyspace,
	// filtering out any ranges that we can't translate.
	var splitPoints [][]byte
	for rangeDescIterator.Valid() && copySplitsFromIndexID != 0 {
		rangeDesc := rangeDescIterator.CurRangeDescriptor()
		rangeDescIterator.Next()
		// For every range's start key, translate the start key into the keyspace
		// of the replacement index. We'll split the replacement index along this
		// same boundary later.
		startKey := rangeDesc.StartKey

		restOfKey, foundTableID, foundIndexID, err := is.codec.DecodeIndexPrefix(roachpb.Key(startKey))
		if err != nil {
			// If we get an error here, it means that either our key didn't contain
			// an index ID (because it was the first range in a table) or the key
			// didn't contain a table ID (because it's still the first range in the
			// system that hasn't split off yet).
			// In this case, we can't translate this range into the new keyspace,
			// so we just have to continue along.
			continue
		}
		if descpb.ID(foundTableID) != tableID {
			// We found a split point that started somewhere else in the database,
			// so we can't translate it to the new keyspace. Don't bother with this
			// range.
			continue
		}
		if descpb.IndexID(foundIndexID) != copySplitsFromIndexID {
			// We found a split point that is not for the primary index. We don't
			// want to copy split points from other indexes.
			continue
		}

		newStartKey := append(is.codec.IndexPrefix(uint32(tableID), uint32(indexToBackfill.GetID())), restOfKey...)
		splitPoints = append(splitPoints, newStartKey)
	}

	if len(splitPoints) == 0 {
		splitPoints, err = is.getSplitPointsWithStats(ctx, table, indexToBackfill, nSplits)
		if err != nil {
			log.Warningf(ctx, "unable to get split points for stats for tableID=%d index=%d due to %v", tableID, indexToBackfill.GetID(), err)
		}
	}

	if len(splitPoints) == 0 {
		// If we can't sample splits from another index, just add one split.
		log.Infof(ctx, "making a single split point in tableId=%d index=%d", tableID, indexToBackfill.GetID())
		span := table.IndexSpan(is.codec, indexToBackfill.GetID())
		expirationTime := is.db.Clock().Now().Add(backfillSplitExpiration.Nanoseconds(), 0)
		splitKey, err := keys.EnsureSafeSplitKey(span.Key)
		if err != nil {
			return err
		}
		// Execute the testing knob before adding a split.
		if is.testingKnobs.BeforeIndexSplitAndScatter != nil {
			is.testingKnobs.BeforeIndexSplitAndScatter([][]byte{splitKey})
		}
		// We split without scattering here because there is only one split point,
		// so scattering wouldn't spread that much load.
		return is.db.AdminSplit(ctx, splitKey, expirationTime)
	}

	// Finally, downsample the split points - choose just nSplits of them to keep.
	actualSplits := min(nSplits, len(splitPoints))
	log.Infof(ctx, "making %d split points in index=%d sampled from (table=%d index=%d)",
		actualSplits, indexToBackfill.GetID(), tableID, copySplitsFromIndexID)
	step := float64(len(splitPoints)) / float64(nSplits)
	if step < 1 {
		step = 1
	}
	// Execute the testing knob before the split and scatter.
	if is.testingKnobs.BeforeIndexSplitAndScatter != nil {
		is.testingKnobs.BeforeIndexSplitAndScatter(splitPoints)
	}
	for i := 0; i < nSplits; i++ {
		// Evenly space out the ranges that we select from the ranges that are
		// returned.
		idx := int(step * float64(i))
		if idx >= len(splitPoints) {
			break
		}
		sp := splitPoints[idx]

		// Jitter the expiration time by 20% up or down from the default.
		maxJitter := backfillSplitExpiration.Nanoseconds() / 5
		jitter := rand.Int63n(maxJitter*2) - maxJitter
		expirationTime := backfillSplitExpiration.Nanoseconds() + jitter

		b.AddRawRequest(&kvpb.AdminSplitRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: sp,
			},
			SplitKey:       sp,
			ExpirationTime: is.db.Clock().Now().Add(expirationTime, 0),
		})
	}
	if err = is.db.Run(ctx, b); err != nil {
		return err
	}

	// If there were a non-trivial number of splits, then scatter the ranges
	// after we've finished splitting them.
	if actualSplits > nNodes {
		log.Infof(ctx, "scattering %d split points in index=%d sampled from (table=%d index=%d)",
			actualSplits, indexToBackfill.GetID(), tableID, copySplitsFromIndexID)
		b = is.db.NewBatch()
		b.AddRawRequest(&kvpb.AdminScatterRequest{
			// Scatter all of the data in the new index.
			RequestHeader: kvpb.RequestHeader{
				Key:    is.codec.IndexPrefix(uint32(tableID), uint32(indexToBackfill.GetID())),
				EndKey: is.codec.IndexPrefix(uint32(tableID), uint32(indexToBackfill.GetID())).PrefixEnd(),
			},
			RandomizeLeases: true,
		})
		return is.db.Run(ctx, b)
	}
	return nil
}

func (is *indexSplitAndScatter) shouldSplitAndScatter(idx catalog.Index) bool {
	if idx == nil {
		return false
	}

	if idx.Adding() && idx.IsSharded() {
		return idx.Backfilling() || (idx.IsTemporaryIndexForBackfill() && idx.DeleteOnly())
	}
	return false
}

// MaybeSplitIndexSpansForPartitioning implements the scexec.IndexSpanSplitter interface.
func (is *indexSplitAndScatter) MaybeSplitIndexSpansForPartitioning(
	ctx context.Context, tableDesc catalog.TableDescriptor, idx catalog.Index,
) error {
	if !is.shouldSplitAndScatter(idx) {
		return nil
	}
	const backfillSplitExpiration = time.Hour
	expirationTime := is.db.Clock().Now().Add(backfillSplitExpiration.Nanoseconds(), 0) // Iterate through all partitioning lists to get all possible list

	// partitioning key prefix. Hash sharded index only allows implicit
	// partitioning, and implicit partitioning does not support
	// subpartition. So it's safe not to consider subpartitions. Range
	// partition is not considered here as well, because it's hard to
	// predict the sampling points within each range to make the pre-split
	// on shard boundaries helpful.
	var partitionKeyPrefixes []roachpb.Key
	partitioning := idx.GetPartitioning()
	if err := partitioning.ForEachList(
		func(name string, values [][]byte, subPartitioning catalog.Partitioning) error {
			for _, tupleBytes := range values {
				_, key, err := rowenc.DecodePartitionTuple(
					&tree.DatumAlloc{},
					is.codec,
					tableDesc,
					idx,
					partitioning,
					tupleBytes,
					tree.Datums{},
				)
				if err != nil {
					return err
				}
				partitionKeyPrefixes = append(partitionKeyPrefixes, key)
			}
			return nil
		},
	); err != nil {
		return err
	}
	splitAtShards := calculateSplitAtShards(maxHashShardedIndexRangePreSplit.Get(is.sv), idx.GetSharded().ShardBuckets)
	if len(partitionKeyPrefixes) == 0 {
		// If there is no partitioning on the index, only pre-split on
		// selected shard boundaries.
		for _, shard := range splitAtShards {
			keyPrefix := is.codec.IndexPrefix(uint32(tableDesc.GetID()), uint32(idx.GetID()))
			splitKey := encoding.EncodeVarintAscending(keyPrefix, shard)
			if err := splitAndScatter(ctx, is.db, splitKey, expirationTime); err != nil {
				return err
			}
		}
	} else {
		// If there are partitioning prefixes, pre-split each of them.
		for _, partPrefix := range partitionKeyPrefixes {
			for _, shard := range splitAtShards {
				// Ensure that we don't reuse memory that came from the
				// descriptor.
				keyPrefix := partPrefix[:len(partPrefix):len(partPrefix)]
				splitKey := encoding.EncodeVarintAscending(keyPrefix, shard)
				if err := splitAndScatter(ctx, is.db, splitKey, expirationTime); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
