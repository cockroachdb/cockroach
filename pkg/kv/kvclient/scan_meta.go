// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvclient

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ScanMetaKVs returns the meta KVs for the ranges that touch the given span.
func ScanMetaKVs(ctx context.Context, txn *kv.Txn, span roachpb.Span) ([]kv.KeyValue, error) {
	metaStart := keys.RangeMetaKey(keys.MustAddr(span.Key).Next())
	metaEnd := keys.RangeMetaKey(keys.MustAddr(span.EndKey))

	kvs, err := txn.Scan(ctx, metaStart, metaEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 || !kvs[len(kvs)-1].Key.Equal(metaEnd.AsRawKey()) {
		// Normally we need to scan one more KV because the ranges are addressed by
		// the end key.
		extraKV, err := txn.Scan(ctx, metaEnd, keys.Meta2Prefix.PrefixEnd(), 1 /* one result */)
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, extraKV[0])
	}
	return kvs, nil
}

// GetRangeWithID returns the RangeDescriptor with the requested id, or nil if
// no such range exists. Note that it performs a scan over the meta.
func GetRangeWithID(
	ctx context.Context, txn *kv.Txn, id roachpb.RangeID,
) (*roachpb.RangeDescriptor, error) {
	// Scan the range meta K/V's to find the target range. We do this in a
	// chunk-wise fashion to avoid loading all ranges into memory.
	var ranges []kv.KeyValue
	var err error
	var rangeDesc roachpb.RangeDescriptor
	const chunkSize = 100
	metaStart := keys.RangeMetaKey(keys.MustAddr(keys.MinKey).Next())
	metaEnd := keys.MustAddr(keys.Meta2Prefix.PrefixEnd())
	for {
		// Scan a batch of ranges.
		ranges, err = txn.Scan(ctx, metaStart, metaEnd, chunkSize)
		if err != nil {
			return nil, err
		}
		// If no results were returned, then exit.
		if len(ranges) == 0 {
			break
		}
		for _, r := range ranges {
			if err := r.ValueProto(&rangeDesc); err != nil {
				return nil, err
			}
			// Look for a range that matches the target range ID.
			if rangeDesc.RangeID == id {
				return &rangeDesc, nil
			}
		}
		// Set the next starting point to after the last key in this batch.
		metaStart = keys.MustAddr(ranges[len(ranges)-1].Key.Next())
	}
	return nil, nil
}
