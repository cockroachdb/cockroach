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
	index atomic.Uint64
}

// Init initializes the sequencer from the last WAG node index persisted in the
// log engine, ensuring that subsequent calls to Next return indices above all
// existing WAG nodes. Must be called before any calls to Next, typically during
// store startup.
func (s *Seq) Init(ctx context.Context, logEngine storage.Reader) error {
	prefix := keys.StoreWAGPrefix()
	prefixEnd := prefix.PrefixEnd()
	mi, err := logEngine.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixEnd,
	})
	if err != nil {
		return err
	}
	defer mi.Close()
	mi.SeekLT(storage.MakeMVCCMetadataKey(prefixEnd))
	if ok, err := mi.Valid(); err != nil {
		return err
	} else if !ok {
		return nil // no WAG nodes; index stays at 0
	}
	index, err := keys.DecodeWAGNodeKey(mi.UnsafeKey().Key)
	if err != nil {
		return err
	}
	s.index.Store(index)
	return nil
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
	data, err := node.Marshal() // nolint:protomarshal
	if err != nil {
		return err
	}
	return w.PutUnversioned(keys.StoreWAGNodeKey(index), data)
}

// Delete removes the WAG node at the given sequence number.
func Delete(w storage.Writer, index uint64) error {
	return w.ClearUnversioned(keys.StoreWAGNodeKey(index), storage.ClearOptions{})
}

// Iterator helps to scan the WAG sequence.
//
//	var iter wag.Iterator
//	for index, node := range iter.Iter(ctx, reader) {
//		// process index, node
//	}
//	if err := iter.Error(); err != nil {
//		return err
//	}
//
// TODO(pav-kv): make it more flexible, e.g. iterate from a particular index.
type Iterator struct {
	// err is the last error encountered during the WAG iteration.
	err error
}

// Iter returns an iterator that scans the WAG sequence. The iterator yields a
// pair containing the WAG node index and the WAG node itself.
func (it *Iterator) Iter(ctx context.Context, r storage.Reader) iter.Seq2[uint64, wagpb.Node] {
	prefix := keys.StoreWAGPrefix()
	mi, err := r.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
		UpperBound: prefix.PrefixEnd(),
	})
	if err != nil {
		it.err = err
		return nil
	}
	mi.SeekGE(storage.MakeMVCCMetadataKey(prefix))

	return func(yield func(uint64, wagpb.Node) bool) {
		defer mi.Close()
		for ; ; mi.Next() {
			if ok, err := mi.Valid(); err != nil || !ok {
				it.err = err
				return
			}
			index, err := keys.DecodeWAGNodeKey(mi.UnsafeKey().Key)
			if err != nil {
				it.err = err
				return
			}
			v, err := mi.UnsafeValue()
			if err != nil {
				it.err = err
				return
			}
			var node wagpb.Node
			if it.err = node.Unmarshal(v); it.err != nil { // nolint:protounmarshal
				return
			}
			if !yield(index, node) {
				return
			}
		}
	}
}

// Error returns the last encountered error during the iteration.
func (it *Iterator) Error() error {
	return it.err
}
