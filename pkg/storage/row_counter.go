// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// RowCounter is a helper that counts how many distinct rows appear in the KVs
// that is shown via `Count`. Note: the `DataSize` field of the BulkOpSummary
// is *not* populated by this and should be set separately.
type RowCounter struct {
	kvpb.BulkOpSummary
	prev roachpb.Key
}

// Count examines each key passed to it and increments the running count when it
// sees a key that belongs to a new row.
func (r *RowCounter) Count(key roachpb.Key) error {
	// EnsureSafeSplitKey is usually used to avoid splitting a row across ranges,
	// by returning the row's key prefix.
	// We reuse it here to count "rows" by counting when it changes.
	// Non-SQL keys are returned unchanged or may error -- we ignore them, since
	// non-SQL keys are obviously thus not SQL rows.
	//
	// TODO(ajwerner): provide a separate mechanism to determine whether the key
	// is a valid SQL key which explicitly indicates whether the key is valid as
	// a split key independent of an error. See #43423.
	row, err := keys.EnsureSafeSplitKey(key)
	if err != nil || len(key) == len(row) {
		// TODO(ajwerner): Determine which errors should be ignored and only
		// ignore those.
		return nil //nolint:returnerrcheck
	}

	// no change key prefix => no new row.
	if bytes.Equal(row, r.prev) {
		return nil
	}

	r.prev = append(r.prev[:0], row...)

	rem, _, err := keys.DecodeTenantPrefix(row)
	if err != nil {
		return err
	}
	_, tableID, indexID, err := keys.DecodeTableIDIndexID(rem)
	if err != nil {
		return err
	}

	if r.EntryCounts == nil {
		r.EntryCounts = make(map[uint64]int64)
	}
	r.EntryCounts[kvpb.BulkOpSummaryID(uint64(tableID), uint64(indexID))]++

	return nil
}
