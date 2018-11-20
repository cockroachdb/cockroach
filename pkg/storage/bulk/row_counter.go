// Copyright 2017 The Cockroach Authors.
//
/// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
