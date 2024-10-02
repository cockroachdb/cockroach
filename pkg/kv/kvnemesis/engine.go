// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// Engine is a simplified version of storage.ReadWriter. It is a multi-version
// key-value map, meaning that each read or write has an associated timestamp
// and a read returns the write for the key with the highest timestamp (which is
// not necessarily the most recently ingested write). Engine is not threadsafe.
type Engine struct {
	kvs *pebble.DB
	b   bufalloc.ByteAllocator
}

// MakeEngine returns a new Engine.
func MakeEngine() (*Engine, error) {
	opts := storage.DefaultPebbleOptions()
	opts.FormatMajorVersion = pebble.FormatNewest // for range key deletions
	opts.FS = vfs.NewMem()
	kvs, err := pebble.Open(`kvnemesis`, opts)
	if err != nil {
		return nil, err
	}
	return &Engine{kvs: kvs}, nil
}

// Close closes the Engine, freeing associated resources.
func (e *Engine) Close() {
	if err := e.kvs.Close(); err != nil {
		panic(err)
	}
}

// Get returns the value for this key with the highest timestamp <= ts. If no
// such value exists, the returned value's RawBytes is nil.
func (e *Engine) Get(key roachpb.Key, ts hlc.Timestamp) roachpb.Value {
	opts := pebble.IterOptions{
		KeyTypes: pebble.IterKeyTypePointsAndRanges,
		// Make MVCC range deletions actually appear to delete points in
		// this low-level iterator, so we don't have to implement it manually
		// a second time.
		RangeKeyMasking: pebble.RangeKeyMasking{
			Suffix: storage.EncodeMVCCTimestampSuffix(ts),
		},
	}
	iter, err := e.kvs.NewIter(&opts)
	if err != nil {
		panic(err)
	}
	defer func() { _ = iter.Close() }()
	iter.SeekGE(storage.EncodeMVCCKey(storage.MVCCKey{Key: key, Timestamp: ts}))
	for iter.Valid() {
		hasPoint, _ := iter.HasPointAndRange()
		if !hasPoint {
			iter.Next()
		} else {
			break
		}
	}
	if !iter.Valid() {
		return roachpb.Value{}
	}

	// We're on the first point the iter is seeing.

	// This use of iter.Key() is safe because it comes entirely before the
	// deferred iter.Close.
	mvccKey, err := storage.DecodeMVCCKey(iter.Key())
	if err != nil {
		panic(err)
	}
	if !mvccKey.Key.Equal(key) {
		return roachpb.Value{}
	}
	var valCopy []byte
	v, err := iter.ValueAndErr()
	if err != nil {
		panic(err)
	}
	e.b, valCopy = e.b.Copy(v, 0 /* extraCap */)
	mvccVal, err := storage.DecodeMVCCValue(valCopy)
	if err != nil {
		panic(err)
	}
	if mvccVal.IsTombstone() {
		return roachpb.Value{}
	}
	val := mvccVal.Value
	val.Timestamp = mvccKey.Timestamp
	return val
}

// Put inserts a key/value/timestamp tuple. If an exact key/timestamp pair is
// Put again, it overwrites the previous value.
func (e *Engine) Put(key storage.MVCCKey, value []byte) {
	if err := e.kvs.Set(storage.EncodeMVCCKey(key), value, nil); err != nil {
		panic(err)
	}
}

func (e *Engine) DeleteRange(from, to roachpb.Key, ts hlc.Timestamp, val []byte) {
	suffix := storage.EncodeMVCCTimestampSuffix(ts)
	err := e.kvs.RangeKeySet(
		storage.EngineKey{Key: from}.Encode(), storage.EngineKey{Key: to}.Encode(), suffix, val, nil)
	if err != nil {
		panic(err)
	}
}

// Iterate calls the given closure with every KV in the Engine, in ascending
// order.
func (e *Engine) Iterate(
	fn func(key, endKey roachpb.Key, ts hlc.Timestamp, value []byte, err error),
) {
	iter, err := e.kvs.NewIter(&pebble.IterOptions{KeyTypes: pebble.IterKeyTypePointsAndRanges})
	if err != nil {
		fn(nil, nil, hlc.Timestamp{}, nil, err)
		return
	}
	defer func() { _ = iter.Close() }()
	for iter.First(); iter.Valid(); iter.Next() {
		hasPoint, _ := iter.HasPointAndRange()
		var keyCopy, valCopy []byte
		e.b, keyCopy = e.b.Copy(iter.Key(), 0 /* extraCap */)
		v, err := iter.ValueAndErr()
		if err != nil {
			fn(nil, nil, hlc.Timestamp{}, nil, err)
		}
		e.b, valCopy = e.b.Copy(v, 0 /* extraCap */)
		if hasPoint {
			key, err := storage.DecodeMVCCKey(keyCopy)
			if err != nil {
				fn(nil, nil, hlc.Timestamp{}, nil, err)
			} else {
				fn(key.Key, nil, key.Timestamp, valCopy, nil)
			}
		}
		if iter.RangeKeyChanged() {
			keyCopy, endKeyCopy := iter.RangeBounds()
			e.b, keyCopy = e.b.Copy(keyCopy, 0 /* extraCap */)
			e.b, endKeyCopy = e.b.Copy(endKeyCopy, 0 /* extraCap */)
			for _, rk := range iter.RangeKeys() {
				ts, err := storage.DecodeMVCCTimestampSuffix(rk.Suffix)
				if err != nil {
					fn(nil, nil, hlc.Timestamp{}, nil, err)
					continue
				}
				engineKey, ok := storage.DecodeEngineKey(keyCopy)
				if !ok || len(engineKey.Version) > 0 {
					fn(nil, nil, hlc.Timestamp{}, nil, errors.Errorf("invalid key %q", keyCopy))
				}
				engineEndKey, ok := storage.DecodeEngineKey(endKeyCopy)
				if !ok || len(engineEndKey.Version) > 0 {
					fn(nil, nil, hlc.Timestamp{}, nil, errors.Errorf("invalid key %q", endKeyCopy))
				}

				e.b, rk.Value = e.b.Copy(rk.Value, 0)

				fn(engineKey.Key, engineEndKey.Key, ts, rk.Value, nil)
			}
		}
	}

	if err := iter.Error(); err != nil {
		fn(nil, nil, hlc.Timestamp{}, nil, err)
	}
}

// DebugPrint returns the entire contents of this Engine as a string for use in
// debugging.
func (e *Engine) DebugPrint(indent string) string {
	var buf strings.Builder
	e.Iterate(func(key, endKey roachpb.Key, ts hlc.Timestamp, value []byte, err error) {
		if buf.Len() > 0 {
			buf.WriteString("\n")
		}
		if err != nil {
			fmt.Fprintf(&buf, "(err:%s)", err)
		} else {
			v, err := storage.DecodeMVCCValue(value)
			if err != nil {
				fmt.Fprintf(&buf, "(err:%s)", err)
				return
			}
			if len(endKey) == 0 {
				fmt.Fprintf(&buf, "%s%s %s -> %s",
					indent, key, ts, v.Value.PrettyPrint())
			} else {
				fmt.Fprintf(&buf, "%s%s-%s %s -> %s",
					indent, key, endKey, ts, v.Value.PrettyPrint())
			}
		}
	})
	return buf.String()
}
