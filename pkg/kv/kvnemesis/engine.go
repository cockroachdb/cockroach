// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	iter := e.kvs.NewIter(nil)
	defer func() { _ = iter.Close() }()
	iter.SeekGE(storage.EncodeKey(storage.MVCCKey{Key: key, Timestamp: ts}))
	if !iter.Valid() {
		return roachpb.Value{}
	}
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
	e.b, valCopy = e.b.Copy(iter.Value(), 0 /* extraCap */)
	return roachpb.Value{RawBytes: valCopy, Timestamp: mvccKey.Timestamp}
}

// Put inserts a key/value/timestamp tuple. If an exact key/timestamp pair is
// Put again, it overwrites the previous value.
func (e *Engine) Put(key storage.MVCCKey, value []byte) {
	if err := e.kvs.Set(storage.EncodeKey(key), value, nil); err != nil {
		panic(err)
	}
}

// Iterate calls the given closure with every KV in the Engine, in ascending
// order.
func (e *Engine) Iterate(fn func(key storage.MVCCKey, value []byte, err error)) {
	iter := e.kvs.NewIter(nil)
	defer func() { _ = iter.Close() }()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := iter.Error(); err != nil {
			fn(storage.MVCCKey{}, nil, err)
			continue
		}
		var keyCopy, valCopy []byte
		e.b, keyCopy = e.b.Copy(iter.Key(), 0 /* extraCap */)
		e.b, valCopy = e.b.Copy(iter.Value(), 0 /* extraCap */)
		key, err := storage.DecodeMVCCKey(keyCopy)
		if err != nil {
			fn(storage.MVCCKey{}, nil, err)
			continue
		}
		fn(key, valCopy, nil)
	}
}

// DebugPrint returns the entire contents of this Engine as a string for use in
// debugging.
func (e *Engine) DebugPrint(indent string) string {
	var buf strings.Builder
	e.Iterate(func(key storage.MVCCKey, value []byte, err error) {
		if buf.Len() > 0 {
			buf.WriteString("\n")
		}
		if err != nil {
			fmt.Fprintf(&buf, "(err:%s)", err)
		} else {
			fmt.Fprintf(&buf, "%s%s %s -> %s",
				indent, key.Key, key.Timestamp, roachpb.Value{RawBytes: value}.PrettyPrint())
		}
	})
	return buf.String()
}
