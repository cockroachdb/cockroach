// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/RaduBerinde/btreemap"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// IterationOrder specifies the order in which replicas will be iterated through
// by VisitKeyRange.
type IterationOrder int

// Ordering options for VisitKeyRange.
const (
	AscendingKeyOrder  = IterationOrder(-1)
	DescendingKeyOrder = IterationOrder(1)
)

// replicaOrPlaceholder represents an element in the btree, with convenience
// fields indicating whether this is a *Replica or a *ReplicaPlaceholder.
// Exactly one of these two fields is set. The zero replicaOrPlaceholder represents no
// value.
type replicaOrPlaceholder struct {
	repl *Replica
	ph   *ReplicaPlaceholder
}

func (it replicaOrPlaceholder) Desc() *roachpb.RangeDescriptor {
	if it.repl != nil {
		return it.repl.Desc()
	}
	return it.ph.Desc()
}

// isEmpty returns true if the replicaOrPlacholder is empty.
func (it replicaOrPlaceholder) isEmpty() bool {
	return it.repl == nil && it.ph == nil
}

func (it replicaOrPlaceholder) item() interface{} {
	if it.repl != nil {
		return it.repl
	}
	if it.ph != nil {
		return it.ph
	}
	return nil
}

func (it replicaOrPlaceholder) key() roachpb.RKey {
	if it.repl != nil {
		return it.repl.startKey
	}
	if it.ph != nil {
		return it.ph.Desc().StartKey
	}
	return nil
}

// storeReplicaBTree is a wrapper around a Btree specialized for use as
// a Store's replicasByKey btree. Please use only the "exported" (upper-case)
// methods; everything else should be considered internal and only for use
// from within storeReplicaBTree.
type storeReplicaBTree btreemap.BTreeMap[roachpb.RKey, replicaOrPlaceholder]

func newStoreReplicaBTree() *storeReplicaBTree {
	m := btreemap.New[roachpb.RKey, replicaOrPlaceholder](64 /* degree */, roachpb.RKey.Compare)
	return (*storeReplicaBTree)(m)
}

// LookupReplica returns the *Replica containing the specified key,
// or nil if no such Replica exists. Placeholders are never returned.
func (b *storeReplicaBTree) LookupReplica(ctx context.Context, key roachpb.RKey) *Replica {
	var repl *Replica
	b.mustDescendLessOrEqual(ctx, key, func(_ context.Context, it replicaOrPlaceholder) error {
		if it.repl != nil {
			repl = it.repl
			return iterutil.StopIteration()
		}
		return nil
	})
	if repl == nil || !repl.Desc().ContainsKey(key) {
		return nil
	}
	return repl
}

// LookupPrecedingReplica returns the Replica (if any) preceding the specified key,
// meaning that the returned replica will not contain the key but will be that key's
// rightmost left neighbor. Placeholders are ignored.
func (b *storeReplicaBTree) LookupPrecedingReplica(ctx context.Context, key roachpb.RKey) *Replica {
	var repl *Replica
	b.mustDescendLessOrEqual(ctx, key, func(_ context.Context, it replicaOrPlaceholder) error {
		if it.repl != nil && !it.repl.ContainsKey(key.AsRawKey()) {
			repl = it.repl
			return iterutil.StopIteration()
		}
		return nil
	})
	return repl
}

// LookupNextReplica returns the first / leftmost Replica (if any) whose start
// key is greater or equal to the provided key. Placeholders are ignored.
func (b *storeReplicaBTree) LookupNextReplica(ctx context.Context, key roachpb.RKey) *Replica {
	var repl *Replica
	if err := b.ascendRange(ctx, key, roachpb.RKeyMax, func(_ context.Context, it replicaOrPlaceholder) error {
		if it.repl != nil {
			repl = it.repl
			return iterutil.StopIteration()
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return repl
}

// VisitKeyRange invokes the visitor on all the replicas for ranges that
// overlap [startKey, endKey), or until the visitor returns false, in the
// specific order. (The items in the btree are non-overlapping).
//
// The argument to the visitor is either a *Replica or *ReplicaPlaceholder.
// Returned replicas might be IsDestroyed(); if the visitor cares, it needs to
// protect against it itself.
//
// The visitor can return iterutil.StopIteration to gracefully stop iterating,
// without an error returned from this method.
func (b *storeReplicaBTree) VisitKeyRange(
	ctx context.Context,
	startKey, endKey roachpb.RKey,
	order IterationOrder,
	visitor func(context.Context, replicaOrPlaceholder) error,
) error {
	if endKey.Less(startKey) {
		return errors.AssertionFailedf("endKey < startKey (%s < %s)", endKey, startKey)
	}
	// Align startKey on a range start. Otherwise the ascendRange below would skip
	// startKey's range.
	for k, r := range b.bt().Descend(btreemap.LE(startKey), btreemap.Min[roachpb.RKey]()) {
		desc := r.Desc()
		if startKey.Less(desc.EndKey) {
			// We are in the situation below, and want to
			// use desc.StartKey instead of startKey going
			// forward.
			//
			//
			//  desc.StartKey   startKey     desc.EndKey
			//      |                |           |
			//      v                v           v
			//  ----:----------------:-----------:--
			startKey = k
		} else {
			// The previous range does not actually overlap
			// start key, so we hold on to our original startKey.
			//
			//  desc.StartKey   desc.EndKey startKey
			//      |                |           |
			//      v                v           v
			//  ----:----------------:-----------:--
		}
		break
	}

	// Iterate through overlapping replicas.
	if order == AscendingKeyOrder {
		return b.ascendRange(ctx, startKey, endKey, visitor)
	}

	// Note that we can't use DescendRange() because it treats the lower end as
	// exclusive and the high end as inclusive.

	for _, r := range b.bt().Descend(btreemap.LT(endKey), btreemap.Min[roachpb.RKey]()) {
		desc := r.Desc()
		if desc.EndKey.Compare(startKey) <= 0 {
			// Stop when we hit a range below startKey.
			return nil
		}
		if err := visitor(ctx, r); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// DeleteReplica deletes a *Replica from the btree, returning it as an replicaOrPlaceholder if
// found.
func (b *storeReplicaBTree) DeleteReplica(ctx context.Context, repl *Replica) replicaOrPlaceholder {
	_, r, _ := b.bt().Delete(repl.startKey)
	return r
}

// DeletePlaceholder deletes a *ReplicaPlaceholder from the btree, returning it as
// an replicaOrPlaceholder if found.
func (b *storeReplicaBTree) DeletePlaceholder(
	ctx context.Context, ph *ReplicaPlaceholder,
) replicaOrPlaceholder {
	_, r, _ := b.bt().Delete(ph.Desc().StartKey)
	return r
}

// ReplaceOrInsertReplica inserts a *Replica into the btree. If it replaces an
// existing element, that element is returned.
func (b *storeReplicaBTree) ReplaceOrInsertReplica(
	ctx context.Context, repl *Replica,
) replicaOrPlaceholder {
	_, r, _ := b.bt().ReplaceOrInsert(repl.startKey, replicaOrPlaceholder{repl: repl})
	return r
}

// ReplaceOrInsertPlaceholder inserts a *ReplicaPlaceholder into the btree. If
// it replaces an existing element, that element is returned.
func (b *storeReplicaBTree) ReplaceOrInsertPlaceholder(
	ctx context.Context, ph *ReplicaPlaceholder,
) replicaOrPlaceholder {
	_, r, _ := b.bt().ReplaceOrInsert(ph.Desc().StartKey, replicaOrPlaceholder{ph: ph})
	return r
}

func (b *storeReplicaBTree) mustDescendLessOrEqual(
	ctx context.Context,
	startKey roachpb.RKey,
	visitor func(context.Context, replicaOrPlaceholder) error,
) {
	if err := b.descendLessOrEqual(ctx, startKey, visitor); err != nil {
		panic(err)
	}
}

func (b *storeReplicaBTree) descendLessOrEqual(
	ctx context.Context,
	startKey roachpb.RKey,
	visitor func(context.Context, replicaOrPlaceholder) error,
) error {
	for _, r := range b.bt().Descend(btreemap.LE(startKey), btreemap.Min[roachpb.RKey]()) {
		if err := visitor(ctx, r); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

func (b *storeReplicaBTree) ascendRange(
	ctx context.Context,
	startKey, endKey roachpb.RKey,
	visitor func(context.Context, replicaOrPlaceholder) error,
) error {
	for _, r := range b.bt().Ascend(btreemap.GE(startKey), btreemap.LT(endKey)) {
		if err := visitor(ctx, r); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

func (b *storeReplicaBTree) bt() *btreemap.BTreeMap[roachpb.RKey, replicaOrPlaceholder] {
	return (*btreemap.BTreeMap[roachpb.RKey, replicaOrPlaceholder])(b)
}

// SafeFormat implements redact.SafeFormatter.
func (b *storeReplicaBTree) SafeFormat(w redact.SafePrinter, _ rune) {
	var i int
	for _, r := range b.bt().Ascend(btreemap.Min[roachpb.RKey](), btreemap.Max[roachpb.RKey]()) {
		if i > -1 {
			w.SafeString("\n")
		}
		desc := r.Desc()
		w.Printf("%v - %v (%s - %s)", []byte(desc.StartKey), []byte(desc.EndKey), desc.StartKey, desc.EndKey)
		i++
	}
}

// String implements fmt.Stringer.
func (b *storeReplicaBTree) String() string {
	return redact.StringWithoutMarkers(b)
}
