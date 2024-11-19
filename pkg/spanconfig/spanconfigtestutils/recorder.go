// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigtestutils

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	ctx context.Context,
	toDelete []spanconfig.Target,
	toUpsert []spanconfig.Record,
	minCommitTS, maxCommitTS hlc.Timestamp,
) error {
	if err := r.underlying.UpdateSpanConfigRecords(
		ctx, toDelete, toUpsert, minCommitTS, maxCommitTS,
	); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, d := range toDelete {
		del, err := spanconfig.Deletion(d)
		if err != nil {
			return err
		}
		r.mu.mutations = append(r.mu.mutations, mutation{
			update:   del,
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

// GetAllSystemSpanConfigsThatApply is part of the spanconfig.KVAccessor
// interface.
func (r *KVAccessorRecorder) GetAllSystemSpanConfigsThatApply(
	ctx context.Context, id roachpb.TenantID,
) ([]roachpb.SpanConfig, error) {
	return r.underlying.GetAllSystemSpanConfigsThatApply(ctx, id)
}

// WithTxn is part of the KVAccessor interface.
func (r *KVAccessorRecorder) WithTxn(context.Context, *kv.Txn) spanconfig.KVAccessor {
	panic("unimplemented")
}

// WithISQLTxn is part of the KVAccessor interface.
func (k *KVAccessorRecorder) WithISQLTxn(context.Context, isql.Txn) spanconfig.KVAccessor {
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
		if !mi.update.GetTarget().Equal(mj.update.GetTarget()) { // sort by target order
			return mi.update.GetTarget().Less(mj.update.GetTarget())
		}

		return mi.update.Deletion() // sort deletes before upserts
	})

	// TODO(irfansharif): We could also print out separators to distinguish
	// between different batches, if any.

	var output strings.Builder
	for _, m := range r.mu.mutations {
		if m.update.Deletion() {
			output.WriteString(fmt.Sprintf("delete %s\n", m.update.GetTarget()))
		} else {
			switch {
			case m.update.GetTarget().IsSpanTarget():
				output.WriteString(fmt.Sprintf("upsert %-35s %s\n", m.update.GetTarget(),
					PrintSpanConfigDiffedAgainstDefaults(m.update.GetConfig())))
			case m.update.GetTarget().IsSystemTarget():
				output.WriteString(fmt.Sprintf("upsert %-35s %s\n", m.update.GetTarget(),
					PrintSystemSpanConfigDiffedAgainstDefault(m.update.GetConfig())))
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
