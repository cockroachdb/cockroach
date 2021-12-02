// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigtestutils

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// KVAccessorRecorder wraps around a KVAccessor and records the mutations
// applied to it.
type KVAccessorRecorder struct {
	underlying spanconfig.KVAccessor

	mu struct {
		syncutil.Mutex
		mutations  []mutation
		batchCount int
	}
}

var _ spanconfig.KVAccessor = &KVAccessorRecorder{}

// NewKVAccessorRecorder returns a new KVAccessorRecorder.
func NewKVAccessorRecorder(underlying spanconfig.KVAccessor) *KVAccessorRecorder {
	return &KVAccessorRecorder{
		underlying: underlying,
	}
}

type mutation struct {
	update   spanconfig.Update
	batchIdx int
}

// GetSpanConfigEntriesFor is part of the KVAccessor interface.
func (r *KVAccessorRecorder) GetSpanConfigEntriesFor(
	ctx context.Context, spans []roachpb.Span,
) ([]roachpb.SpanConfigEntry, error) {
	return r.underlying.GetSpanConfigEntriesFor(ctx, spans)
}

// UpdateSpanConfigEntries is part of the KVAccessor interface.
func (r *KVAccessorRecorder) UpdateSpanConfigEntries(
	ctx context.Context, toDelete []roachpb.Span, toUpsert []roachpb.SpanConfigEntry,
) error {
	if err := r.underlying.UpdateSpanConfigEntries(ctx, toDelete, toUpsert); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, d := range toDelete {
		r.mu.mutations = append(r.mu.mutations, mutation{
			update:   spanconfig.Update{Span: d},
			batchIdx: r.mu.batchCount,
		})
	}
	for _, u := range toUpsert {
		r.mu.mutations = append(r.mu.mutations, mutation{
			update:   spanconfig.Update{Span: u.Span, Config: u.Config},
			batchIdx: r.mu.batchCount,
		})
	}
	r.mu.batchCount++
	return nil
}

// WithTxn is part of the KVAccessor interface.
func (r *KVAccessorRecorder) WithTxn(context.Context, *kv.Txn) spanconfig.KVAccessor {
	panic("unimplemented")
}

// Recording returns a string-ified form of the mutations made so far, i.e. list
// of spans that were deleted and entries that were upserted. It optionally
// clears out the recording.
func (r *KVAccessorRecorder) Recording(clear bool) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	sort.Slice(r.mu.mutations, func(i, j int) bool {
		mi, mj := r.mu.mutations[i], r.mu.mutations[j]
		if mi.batchIdx != mj.batchIdx { // sort by batch/ts order
			return mi.batchIdx < mj.batchIdx
		}
		if !mi.update.Span.Key.Equal(mj.update.Span.Key) { // sort by key order
			return mi.update.Span.Key.Compare(mj.update.Span.Key) < 0
		}

		return mi.update.Deletion() // sort deletes before upserts
	})

	// TODO(irfansharif): We could also print out separators to distinguish
	// between different batches, if any.

	var output strings.Builder
	for _, m := range r.mu.mutations {
		if m.update.Deletion() {
			output.WriteString(fmt.Sprintf("delete %s\n", m.update.Span))
		} else {
			output.WriteString(fmt.Sprintf("upsert %-35s %s\n", m.update.Span,
				PrintSpanConfigDiffedAgainstDefaults(m.update.Config)))
		}
	}

	if clear {
		r.mu.mutations = r.mu.mutations[:0]
		r.mu.batchCount = 0
	}

	return output.String()
}
