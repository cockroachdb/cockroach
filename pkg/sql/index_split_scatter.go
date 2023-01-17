// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

type indexSplitAndScatter struct {
	db    *kv.DB
	codec keys.SQLCodec
	sv    *settings.Values
}

// NewIndexSplitAndScatter creates a new scexec.IndexSpanSplitter implementation.
func NewIndexSplitAndScatter(execCfg *ExecutorConfig) scexec.IndexSpanSplitter {
	return &indexSplitAndScatter{
		db:    execCfg.DB,
		codec: execCfg.Codec,
		sv:    &execCfg.Settings.SV,
	}
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

	// Only perform index span splits on the system tenant and non-temporary
	// indexes.
	if !is.codec.ForSystemTenant() || indexToBackfill.IsTemporaryIndexForBackfill() {
		return nil
	}
	span := table.IndexSpan(is.codec, indexToBackfill.GetID())
	const backfillSplitExpiration = time.Hour
	expirationTime := is.db.Clock().Now().Add(backfillSplitExpiration.Nanoseconds(), 0)
	return is.db.AdminSplit(ctx, span.Key, expirationTime)
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
				splitKey := encoding.EncodeVarintAscending(partPrefix, shard)
				if err := splitAndScatter(ctx, is.db, splitKey, expirationTime); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
