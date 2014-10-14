// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	"code.google.com/p/biogo.store/llrb"
	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
)

// Batch wrap an instance of Engine and provides a limited subset of
// Engine functionality. Mutations are added to a write batch
// transparently and only applied to the wrapped engine on invocation
// of Commit(). Reads are passed through to the wrapped engine. In the
// event that reads access keys for which there are already-batched
// updates, reads from the wrapped engine are combined on the fly with
// pending write, delete, and merge updates.
//
// This struct is not thread safe.
type Batch struct {
	engine    Engine
	updates   llrb.Tree
	committed bool
}

// NewBatch returns a new instance of Batch wrapping engine.
func NewBatch(engine Engine) *Batch {
	return &Batch{
		engine: engine,
	}
}

// Commit writes all pending updates to the underlying engine in
// an atomic write batch.
func (b *Batch) Commit() error {
	if b.committed {
		panic("this batch was already committed")
	}
	var batch []interface{}
	b.updates.DoRange(func(n llrb.Comparable) (done bool) {
		batch = append(batch, n)
		return false
	}, proto.RawKeyValue{Key: KeyMin}, proto.RawKeyValue{Key: KeyMax})
	b.committed = true
	return b.engine.WriteBatch(batch)
}

// Put stores the key / value as a BatchPut in the updates tree.
func (b *Batch) Put(key Key, value []byte) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	b.updates.Insert(BatchPut{proto.RawKeyValue{Key: key, Value: value}})
	return nil
}

// Get reads first from the updates tree. If the key is found there
// and is deleted, then a nil value is returned. If the key is found,
// and is a Put, returns the value from the tree. If a merge, then
// merge is performed on the fly to combine with the value from the
// underlying engine. Otherwise, the Get is simply passed through to
// the wrapped engine.
func (b *Batch) Get(key Key) ([]byte, error) {
	if len(key) == 0 {
		return nil, emptyKeyError()
	}
	val := b.updates.Get(proto.RawKeyValue{Key: key})
	if val != nil {
		switch t := val.(type) {
		case BatchDelete:
			return nil, nil
		case BatchPut:
			return t.Value, nil
		case BatchMerge:
			existingVal, err := b.engine.Get(key)
			if err != nil {
				return nil, err
			}
			return goMerge(existingVal, t.Value)
		}
	}
	return b.engine.Get(key)
}

// iterateUpdates scans the updates tree from start to end, invoking f
// on each value until f returns false or an error.
func (b *Batch) iterateUpdates(start, end Key, f func(proto.RawKeyValue) (bool, error)) (bool, error) {
	var done bool
	var err error
	// Scan the updates tree for the key range, merging as we go.
	b.updates.DoRange(func(n llrb.Comparable) bool {
		switch t := n.(type) {
		case BatchDelete: // On delete, skip.
		case BatchPut: // On put, override the corresponding engine entry.
			done, err = f(t.RawKeyValue)
		case BatchMerge: // On merge, merge with corresponding engine entry.
			kv := proto.RawKeyValue{Key: t.Key}
			kv.Value, err = goMerge([]byte(nil), t.Value)
			if err == nil {
				done, err = f(kv)
			}
		}
		return done || err != nil
	}, proto.RawKeyValue{Key: start}, proto.RawKeyValue{Key: end})
	return done, err
}

// Iterate invokes f on key/value pairs merged from the underlying
// engine and pending batch updates. If f returns done or an error,
// the iteration ends and propagates the error.
//
// TODO(spencer): this implementation could benefit from an
// iterator-style interface to the update map. If/when one is
// provided by the llrb implementation it should be used here
// to make this code more efficient.
func (b *Batch) Iterate(start, end Key, f func(proto.RawKeyValue) (bool, error)) error {
	last := start
	if err := b.engine.Iterate(start, end, func(kv proto.RawKeyValue) (bool, error) {
		// Merge iteration from updates tree at each key/value.
		done, err := b.iterateUpdates(last, kv.Key, f)
		last = Key(kv.Key).Next()
		if !done && err == nil {
			val := b.updates.Get(proto.RawKeyValue{Key: kv.Key})
			if val != nil {
				switch t := val.(type) {
				case BatchDelete:
				case BatchPut:
					f(t.RawKeyValue)
				case BatchMerge:
					mergedKV := proto.RawKeyValue{Key: t.Key}
					mergedKV.Value, err = goMerge(kv.Value, t.Value)
					if err == nil {
						done, err = f(mergedKV)
					}
				}
			} else {
				done, err = f(kv)
			}
		}
		return done, err
	}); err != nil {
		return err
	}
	// Final iteration from updates tree.
	if _, err := b.iterateUpdates(last, end, f); err != nil {
		return err
	}
	return nil
}

// Scan scans from both the updates tree and the underlying engine
// and combines the results, up to max.
func (b *Batch) Scan(start, end Key, max int64) ([]proto.RawKeyValue, error) {
	var kvs []proto.RawKeyValue
	err := b.Iterate(start, end, func(kv proto.RawKeyValue) (bool, error) {
		if max != 0 && int64(len(kvs)) >= max {
			return true, nil
		}
		kvs = append(kvs, kv)
		return false, nil
	})
	return kvs, err
}

// Clear stores the key as a BatchDelete in the updates tree.
func (b *Batch) Clear(key Key) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	b.updates.Insert(BatchDelete{proto.RawKeyValue{Key: key}})
	return nil
}

// Merge stores the key / value as a BatchMerge in the updates tree.
// If the updates map already contains a BatchPut, then this value is
// merged with the Put and kept as a BatchPut. If the updates map
// already contains a BatchMerge, then this value is merged with the
// existing BatchMerge and kept as a BatchMerge. If the updates map
// contains a BatchDelete, then this value is merged with a nil byte
// slice and stored as a BatchPut.
func (b *Batch) Merge(key Key, value []byte) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	val := b.updates.Get(proto.RawKeyValue{Key: key})
	if val != nil {
		switch t := val.(type) {
		case BatchDelete:
			mergedBytes, err := goMerge(nil, value)
			if err != nil {
				return err
			}
			b.updates.Insert(BatchPut{proto.RawKeyValue{Key: key, Value: mergedBytes}})
		case BatchPut:
			mergedBytes, err := goMerge(t.Value, value)
			if err != nil {
				return err
			}
			b.updates.Insert(BatchPut{proto.RawKeyValue{Key: key, Value: mergedBytes}})
		case BatchMerge:
			mergedBytes, err := goMerge(t.Value, value)
			if err != nil {
				return err
			}
			b.updates.Insert(BatchMerge{proto.RawKeyValue{Key: key, Value: mergedBytes}})
		}
	} else {
		b.updates.Insert(BatchMerge{proto.RawKeyValue{Key: key, Value: value}})
	}
	return nil
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg and the provided timestamp. Returns the length in bytes of
// key and the value.
func (b *Batch) PutProto(key Key, msg gogoproto.Message) (keyBytes, valBytes int64, err error) {
	var data []byte
	if data, err = gogoproto.Marshal(msg); err != nil {
		return
	}
	if err = b.Put(key, data); err != nil {
		return
	}
	keyBytes = int64(len(key))
	valBytes = int64(len(data))
	return
}

// GetProto fetches the value at the specified key and unmarshals it
// using a protobuf decoder. Returns true on success or false if the
// key was not found. On success, returns the length in bytes of the
// key and the value.
func (b *Batch) GetProto(key Key, msg gogoproto.Message) (ok bool, keyBytes, valBytes int64, err error) {
	var data []byte
	if data, err = b.Get(key); err != nil {
		return
	}
	if data == nil {
		return
	}
	ok = true
	if msg != nil {
		if err = gogoproto.Unmarshal(data, msg); err != nil {
			return
		}
	}
	keyBytes = int64(len(key))
	valBytes = int64(len(data))
	return
}
