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

package storage

import (
	"bytes"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

type metaAction func([]client.Callable, proto.Key, *proto.RangeDescriptor) []client.Callable

func putMeta(calls []client.Callable, key proto.Key, desc *proto.RangeDescriptor) []client.Callable {
	return append(calls, client.PutProto(key, desc))
}

func delMeta(calls []client.Callable, key proto.Key, desc *proto.RangeDescriptor) []client.Callable {
	return append(calls, client.Delete(key))
}

// splitRangeAddressing creates (or overwrites if necessary) the meta1
// and meta2 range addressing records for the left and right ranges
// caused by a split.
func splitRangeAddressing(left, right *proto.RangeDescriptor) ([]client.Callable, error) {
	var calls []client.Callable
	var err error
	if calls, err = rangeAddressing(calls, left, putMeta); err != nil {
		return nil, err
	}
	if calls, err = rangeAddressing(calls, right, putMeta); err != nil {
		return nil, err
	}
	return calls, nil
}

// mergeRangeAddressing removes subsumed meta1 and meta2 range
// addressing records caused by merging and updates the records for
// the new merged range. Left is the range descriptor for the "left"
// range before merging and merged describes the left to right merge.
func mergeRangeAddressing(left, merged *proto.RangeDescriptor) ([]client.Callable, error) {
	var calls []client.Callable
	var err error
	if calls, err = rangeAddressing(calls, left, delMeta); err != nil {
		return nil, err
	}
	if calls, err = rangeAddressing(calls, merged, putMeta); err != nil {
		return nil, err
	}
	return calls, nil
}

// updateRangeAddressing overwrites the meta1 and meta2 range addressing
// records for the descriptor. Returns a slice of calls necessary to
// update the records on the KV database.
func updateRangeAddressing(desc *proto.RangeDescriptor) ([]client.Callable, error) {
	return rangeAddressing([]client.Callable{}, desc, putMeta)
}

// rangeAddressing updates or deletes the range addressing metadata
// for the range specified by desc. The action to take is specified by
// the supplied metaAction function.
//
// The rules for meta1 and meta2 records are as follows:
//
//  1. If desc.StartKey or desc.EndKey is meta1:
//     - ERROR
//  2. If desc.EndKey is meta2:
//     - meta1(desc.EndKey)
//  3. If desc.EndKey is normal user key:
//     - meta2(desc.EndKey)
//     3a. If desc.StartKey is KeyMin or meta2:
//         - meta1(KeyMax)
func rangeAddressing(calls []client.Callable, desc *proto.RangeDescriptor,
	action metaAction) ([]client.Callable, error) {
	// 1. handle illegal case of start or end key being meta1.
	if bytes.HasPrefix(desc.EndKey, engine.KeyMeta1Prefix) ||
		bytes.HasPrefix(desc.StartKey, engine.KeyMeta1Prefix) {
		return nil, util.Errorf("meta1 addressing records cannot be split: %+v", desc)
	}
	// 2. the case of the range ending with a meta2 prefix. This means
	// the range is full of meta2. We must update the relevant meta1
	// entry pointing to the end of this range.
	if bytes.HasPrefix(desc.EndKey, engine.KeyMeta2Prefix) {
		calls = action(calls, engine.RangeMetaKey(desc.EndKey), desc)
	} else {
		// 3. the range ends with a normal user key, so we must update the
		// relevant meta2 entry pointing to the end of this range.
		calls = action(calls, engine.MakeKey(engine.KeyMeta2Prefix, desc.EndKey), desc)
		// 3a. the range starts with KeyMin or a meta2 addressing record,
		// update the meta1 entry for KeyMax.
		if bytes.Equal(desc.StartKey, engine.KeyMin) ||
			bytes.HasPrefix(desc.StartKey, engine.KeyMeta2Prefix) {
			calls = action(calls, engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax), desc)
		}
	}
	return calls, nil
}
