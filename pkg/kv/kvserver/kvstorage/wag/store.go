// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package wag

import (
	"context"
	"iter"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

// Seq is the WAG sequencer. It allocates consecutive unique sequence numbers to
// assist the writes of the WAG nodes in a topologically sorted order.
type Seq struct {
	// index is the last allocated index into the WAG nodes sequence.
	// TODO(pav-kv): initialize it on store restarts.
	index atomic.Uint64
}

// Next allocates the given number of consecutive WAG nodes in the sequence, and
// returns the index of the first allocated node. The caller can subsequently
// use indices [Next, Next+count) for writing WAG nodes, and must not use
// indices outside this span.
//
// The caller must make sure that conflicting writers call Next and do the
// corresponding writes according to the topological ordering of these
// mutations. For example, replica lifecycle events of a single range must be
// ordered. Independent / concurrent writers can call Next and perform the
// corresponding writes in any order (e.g. different ranges can write their
// events concurrently).
func (s *Seq) Next(count uint64) uint64 {
	return s.index.Add(count) - count + 1
}

// Write puts the WAG node under the specific sequence number into the given
// writer. The index must have been allocated to the caller by the sequencer.
func Write(w storage.Writer, index uint64, node wagpb.Node) error {
	data, err := node.Marshal()
	if err != nil {
		return err
	}
	return w.PutUnversioned(keys.StoreWAGIndexKey(index), data)
}

// Iterator helps to scan the WAG sequence.
type Iterator struct {
	// err is the last error encountered during the WAG iteration.
	err error
}

// Iter returns an iterator that scans the WAG sequence.
func (it *Iterator) Iter(ctx context.Context, r storage.Reader) iter.Seq[wagpb.Node] {
	prefix := keys.StoreWAGPrefix()
	mi, err := r.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
		UpperBound: prefix.PrefixEnd(),
	})
	if err != nil {
		it.err = err
		return nil
	}
	mi.SeekGE(storage.MakeMVCCMetadataKey(prefix))

	return func(yield func(wagpb.Node) bool) {
		defer mi.Close()
		for ; ; mi.Next() {
			if ok, err := mi.Valid(); err != nil || !ok {
				it.err = err
				return
			}
			v, err := mi.UnsafeValue()
			if err != nil {
				it.err = err
				return
			}
			var node wagpb.Node
			if it.err = node.Unmarshal(v); it.err != nil {
				return
			}
			if !yield(node) {
				return
			}
		}
	}
}

// Error returns the last encountered error during the iteration.
func (it *Iterator) Error() error {
	return it.err
}
