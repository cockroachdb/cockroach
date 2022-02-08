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

// GetSpanConfigRecords is part of the KVAccessor interface.
func (r *KVAccessorRecorder) GetSpanConfigRecords(
	ctx context.Context, targets []spanconfig.Target,
) ([]spanconfig.Record, error) {
	return r.underlying.GetSpanConfigRecords(ctx, targets)
}

// UpdateSpanConfigRecords is part of the KVAccessor interface.
func (r *KVAccessorRecorder) UpdateSpanConfigRecords(
	ctx context.Context, toDelete []spanconfig.Target, toUpsert []spanconfig.Record,
) error {
	if err := r.underlying.UpdateSpanConfigRecords(ctx, toDelete, toUpsert); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, d := range toDelete {
		r.mu.mutations = append(r.mu.mutations, mutation{
			update:   spanconfig.Deletion(d),
			batchIdx: r.mu.batchCount,
		})
	}
	for _, u := range toUpsert {
		r.mu.mutations = append(r.mu.mutations, mutation{
			update:   spanconfig.Update(u),
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
		if !mi.update.Target.Equal(mj.update.Target) { // sort by target order
			return mi.update.Target.Less(mj.update.Target)
		}

		return mi.update.Deletion() // sort deletes before upserts
	})

	// TODO(irfansharif): We could also print out separators to distinguish
	// between different batches, if any.

	var output strings.Builder
	for _, m := range r.mu.mutations {
		if m.update.Deletion() {
			output.WriteString(fmt.Sprintf("delete %s\n", m.update.Target))
		} else {
			switch {
			case m.update.Target.IsSpanTarget():
				output.WriteString(fmt.Sprintf("upsert %-35s %s\n", m.update.Target,
					PrintSpanConfigDiffedAgainstDefaults(m.update.Config)))
			case m.update.Target.IsSystemTarget():
				output.WriteString(fmt.Sprintf("upsert %-35s %s\n", m.update.Target,
					PrintSystemSpanConfigDiffedAgainstDefault(m.update.Config)))
			default:
				panic("unsupported target type")
			}
		}
	}

	if clear {
		r.mu.mutations = r.mu.mutations[:0]
		r.mu.batchCount = 0
	}

	return output.String()
}
