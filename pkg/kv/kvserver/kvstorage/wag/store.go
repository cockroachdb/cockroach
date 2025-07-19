package wag

import (
	"context"
	"iter"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

type Writer struct {
	// TODO(pav-kv): initialize it on store restarts.
	index atomic.Uint64
}

func (w *Writer) Next(count uint64) uint64 {
	return w.index.Add(count)
}

func Write(w storage.Writer, index uint64, node wagpb.Node) error {
	data, err := node.Marshal()
	if err != nil {
		return err
	}
	return w.PutUnversioned(keys.StoreWAGIndexKey(index), data)
}

type Iterator struct {
	err error
}

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

func (it *Iterator) Error() error {
	return it.err
}
