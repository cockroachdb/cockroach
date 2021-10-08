// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/google/btree"
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
	item rangeKeyItem
	repl *Replica
	ph   *ReplicaPlaceholder
}

func (it *replicaOrPlaceholder) Desc() *roachpb.RangeDescriptor {
	if it.repl != nil {
		return it.repl.Desc()
	}
	return it.ph.Desc()
}

// storeReplicaBTree is a wrapper around a Btree specialized for use as
// a Store's replicasByKey btree. Please use only the "exported" (upper-case)
// methods; everything else should be considered internal and only for use
// from within storeReplicaBTree.
type storeReplicaBTree btree.BTree

func newStoreReplicaBTree() *storeReplicaBTree {
	return (*storeReplicaBTree)(btree.New(64 /* degree */))
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
	b.mustDescendLessOrEqual(ctx, startKey,
		func(_ context.Context, it replicaOrPlaceholder) error {
			desc := it.Desc()
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
				startKey = it.item.key()
			} else {
				// The previous range does not actually overlap
				// start key, so we hold on to our original startKey.
				//
				//  desc.StartKey   desc.EndKey startKey
				//      |                |           |
				//      v                v           v
				//  ----:----------------:-----------:--
				_ = startKey
			}
			return iterutil.StopIteration()
		})

	// Iterate through overlapping replicas.
	if order == AscendingKeyOrder {
		return b.ascendRange(ctx, startKey, endKey, visitor)
	}

	// Note that we can't use DescendRange() because it treats the lower end as
	// exclusive and the high end as inclusive.
	return b.descendLessOrEqual(ctx, endKey,
		func(ctx context.Context, it replicaOrPlaceholder) error {
			if it.item.(rangeKeyItem).key().Equal(endKey) {
				// Skip the range starting at endKey.
				return nil
			}
			desc := it.Desc()

			if desc.EndKey.Compare(startKey) <= 0 {
				// Stop when we hit a range below startKey.
				return iterutil.StopIteration()
			}
			return visitor(ctx, it)
		})
}

// DeleteReplica deletes a *Replica from the btree, returning it as an replicaOrPlaceholder if
// found.
func (b *storeReplicaBTree) DeleteReplica(ctx context.Context, repl *Replica) replicaOrPlaceholder {
	return itemToReplicaOrPlaceholder(b.bt().Delete((*btreeReplica)(repl)))
}

// DeletePlaceholder deletes a *ReplicaPlaceholder from the btree, returning it as
// an replicaOrPlaceholder if found.
func (b *storeReplicaBTree) DeletePlaceholder(
	ctx context.Context, ph *ReplicaPlaceholder,
) replicaOrPlaceholder {
	return itemToReplicaOrPlaceholder(b.bt().Delete(ph))
}

// ReplaceOrInsertReplica inserts a *Replica into the btree. If it replaces an
// existing element, that element is returned.
func (b *storeReplicaBTree) ReplaceOrInsertReplica(
	ctx context.Context, repl *Replica,
) replicaOrPlaceholder {
	return itemToReplicaOrPlaceholder(b.bt().ReplaceOrInsert((*btreeReplica)(repl)))
}

// ReplaceOrInsertReplica inserts a *ReplicaPlaceholder into the btree. If it
// replaces an existing element, that element is returned.
func (b *storeReplicaBTree) ReplaceOrInsertPlaceholder(
	ctx context.Context, ph *ReplicaPlaceholder,
) replicaOrPlaceholder {
	return itemToReplicaOrPlaceholder(b.bt().ReplaceOrInsert(ph))
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
	var err error
	b.bt().DescendLessOrEqual(rangeBTreeKey(startKey), func(it btree.Item) (more bool) {
		err = visitor(ctx, itemToReplicaOrPlaceholder(it))
		return err == nil // more?
	})
	if iterutil.Done(err) {
		return nil
	}
	return err
}

func (b *storeReplicaBTree) ascendRange(
	ctx context.Context,
	startKey, endKey roachpb.RKey,
	visitor func(context.Context, replicaOrPlaceholder) error,
) error {
	var err error
	b.bt().AscendRange(rangeBTreeKey(startKey), rangeBTreeKey(endKey), func(it btree.Item) (more bool) {
		err = visitor(ctx, itemToReplicaOrPlaceholder(it))
		return err == nil // more
	})
	if iterutil.Done(err) {
		return nil
	}
	return err
}

func (b *storeReplicaBTree) bt() *btree.BTree {
	return (*btree.BTree)(b)
}

// SafeFormat implements redact.SafeFormatter.
func (b *storeReplicaBTree) SafeFormat(w redact.SafePrinter, _ rune) {
	var i int
	b.bt().Ascend(func(bi btree.Item) (more bool) {
		if i > -1 {
			w.SafeString("\n")
		}
		it := itemToReplicaOrPlaceholder(bi)
		desc := it.Desc()
		w.Printf("%v - %v (%s - %s)", []byte(desc.StartKey), []byte(desc.EndKey), desc.StartKey, desc.EndKey)
		i++
		return true
	})
}

// String implements fmt.Stringer.
func (b *storeReplicaBTree) String() string {
	return redact.StringWithoutMarkers(b)
}

// rangeKeyItem is a common interface for *btreeReplica (*Replica),
// *ReplicaPlaceholder, and rangeBTreeKey (roachpb.RKey).
type rangeKeyItem interface {
	key() roachpb.RKey
}

// rangeBTreeKey is a type alias of roachpb.RKey that implements the
// rangeKeyItem interface and the btree.Item interface.
type rangeBTreeKey roachpb.RKey

var _ rangeKeyItem = rangeBTreeKey{}

func (k rangeBTreeKey) key() roachpb.RKey {
	return (roachpb.RKey)(k)
}

var _ btree.Item = rangeBTreeKey{}

func (k rangeBTreeKey) Less(i btree.Item) bool {
	return k.key().Less(i.(rangeKeyItem).key())
}

type btreeReplica Replica

func (r *btreeReplica) key() roachpb.RKey {
	return r.startKey
}

func (r *btreeReplica) Less(i btree.Item) bool {
	return r.key().Less(i.(rangeKeyItem).key())
}

func itemToReplicaOrPlaceholder(it btree.Item) replicaOrPlaceholder {
	if it == nil {
		return replicaOrPlaceholder{}
	}
	vit := replicaOrPlaceholder{
		item: it.(rangeKeyItem),
	}
	switch t := it.(type) {
	case *btreeReplica:
		vit.repl = (*Replica)(t)
	case *ReplicaPlaceholder:
		vit.ph = t
	}
	return vit
}
