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
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/google/btree"
)

// Engine is a simplified version of engine.ReadWriter. It is a multi-version
// key-value map, meaning that each read or write has an associated timestamp
// and a read returns the write for the key with the highest timestamp (which is
// not necessarily the most recently ingested write). Engine is not threadsafe.
//
// TODO(dan): If this gets any more complicated, it should probably just be
// swapped for an in-mem engine.Engine.
type Engine struct {
	kvs *btree.BTree
}

// MakeEngine returns a new Engine.
func MakeEngine() *Engine {
	return &Engine{
		kvs: btree.New(8),
	}
}

// Get returns the value for this key with the highest timestamp <= ts. If no
// such value exists, the returned value's RawBytes is nil.
func (e *Engine) Get(key roachpb.Key, ts hlc.Timestamp) roachpb.Value {
	var value roachpb.Value
	e.kvs.AscendGreaterOrEqual(
		btreeItem{Key: engine.MVCCKey{Key: key, Timestamp: ts}},
		func(i btree.Item) bool {
			if kv := i.(btreeItem); kv.Key.Key.Equal(key) {
				value = roachpb.Value{
					Timestamp: kv.Key.Timestamp,
					RawBytes:  kv.Value,
				}
			}
			return false
		},
	)
	return value
}

// Put inserts a key/value/timestamp tuple. If an exact key/timestamp pair is
// Put again, it overwrites the previous value.
func (e *Engine) Put(key engine.MVCCKey, value []byte) {
	e.kvs.ReplaceOrInsert(btreeItem{Key: key, Value: value})
}

// DebugPrint returns the entire contents of this Engine as a string for use in
// debugging.
func (e *Engine) DebugPrint(indent string) string {
	var buf strings.Builder
	e.kvs.Ascend(func(item btree.Item) bool {
		if buf.Len() > 0 {
			buf.WriteString("\n")
		}
		kv := item.(btreeItem)
		fmt.Fprintf(&buf, "%s%s %s -> %s",
			indent, kv.Key.Key, kv.Key.Timestamp, roachpb.Value{RawBytes: kv.Value}.PrettyPrint())
		return true
	})
	return buf.String()
}

type btreeItem engine.MVCCKeyValue

func (i btreeItem) Less(o btree.Item) bool {
	return i.Key.Less(o.(btreeItem).Key)
}
