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
	"bytes"

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
	engine  Engine
	updates llrb.Tree
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
	var batch []interface{}
	b.updates.DoRange(func(n llrb.Comparable) (done bool) {
		batch = append(batch, n)
		return false
	}, proto.RawKeyValue{Key: KeyMin}, proto.RawKeyValue{Key: KeyMax})
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

// Scan scans from both the updates tree and the underlying engine
// and combines the results, up to max.
func (b *Batch) Scan(start, end Key, max int64) ([]proto.RawKeyValue, error) {
	// First, get up to max key value pairs from the wrapped engine.
	engs, err := b.engine.Scan(start, end, max)
	if err != nil {
		return nil, err
	}
	engIdx := 0
	var kvs []proto.RawKeyValue

	// Now, scan the updates tree for the same range, combining as we go
	// up to max entries.
	b.updates.DoRange(func(n llrb.Comparable) (done bool) {
		// First add all values from engs slice less than the current updates key.
		for engIdx < len(engs) && bytes.Compare(engs[engIdx].Key, n.(proto.KeyGetter).KeyGet()) < 0 {
			kvs = append(kvs, engs[engIdx])
			engIdx++
			if max != 0 && int64(len(kvs)) >= max {
				return true
			}
		}
		engKV := proto.RawKeyValue{Key: KeyMax}
		if engIdx < len(engs) {
			engKV = engs[engIdx]
		}
		switch t := n.(type) {
		case BatchDelete: // On delete, just skip the corresponding engine entry.
			if bytes.Equal(t.Key, engKV.Key) {
				engIdx++
			}
		case BatchPut: // On put, reply the corresponding engine entry.
			if bytes.Equal(t.Key, engKV.Key) {
				engIdx++
			}
			kvs = append(kvs, t.RawKeyValue)
		case BatchMerge: // On merge, merge with corresponding engine entry.
			var existingBytes []byte
			if bytes.Equal(t.Key, engKV.Key) {
				existingBytes = engKV.Value
				engIdx++
			}
			kv := proto.RawKeyValue{Key: t.Key}
			kv.Value, err = goMerge(existingBytes, t.Value)
			if err != nil { // break out of DoRange on error.
				return true
			}
			kvs = append(kvs, kv)
		}
		return max != 0 && int64(len(kvs)) >= max
	}, proto.RawKeyValue{Key: start}, proto.RawKeyValue{Key: end})

	// Check for common case of no matches in the updates map.
	if len(kvs) == 0 {
		return engs, err
	}
	// Otherwise, append remaining entries in engs up to max.
	lastIdx := int64(len(engs))
	if max != 0 {
		if (lastIdx - int64(engIdx)) > max-int64(len(kvs)) {
			lastIdx = max - int64(len(kvs)-engIdx)
		}
	}
	return append(kvs, engs[engIdx:lastIdx]...), err
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
// of msg and the provided timestamp.
func (b *Batch) PutProto(key Key, msg gogoproto.Message) error {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		return err
	}
	return b.Put(key, data)
}

// GetProto fetches the value at the specified key and unmarshals it
// using a protobuf decoder. Returns true on success or false if the
// key was not found.
func (b *Batch) GetProto(key Key, msg gogoproto.Message) (bool, error) {
	val, err := b.Get(key)
	if err != nil {
		return false, err
	}
	if val == nil {
		return false, nil
	}
	if msg != nil {
		if err := gogoproto.Unmarshal(val, msg); err != nil {
			return true, err
		}
	}
	return true, nil
}
