// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// RowCounter is a helper that counts how many distinct rows appear in the KVs
// that is is shown via `Count`. Note: the `DataSize` field of the BulkOpSummary
// is *not* populated by this and should be set separately.
type RowCounter struct {
	roachpb.BulkOpSummary
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
	row, err := keys.EnsureSafeSplitKey(key)
	if err != nil || len(key) == len(row) {
		return nil
	}

	// no change key prefix => no new row.
	if bytes.Equal(row, r.prev) {
		return nil
	}

	r.prev = append(r.prev[:0], row...)

	rest, tbl, err := keys.DecodeTablePrefix(row)
	if err != nil {
		return err
	}

	if tbl < keys.MaxReservedDescID {
		r.SystemRecords++
	} else {
		if _, indexID, err := encoding.DecodeUvarintAscending(rest); err != nil {
			return err
		} else if indexID == 1 {
			r.Rows++
		} else {
			r.IndexEntries++
		}
	}

	return nil
}
