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
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
)

type metaAction func(*client.Batch, proto.Key, *proto.RangeDescriptor)

func putMeta(b *client.Batch, key proto.Key, desc *proto.RangeDescriptor) {
	b.Put(key, desc)
}

func delMeta(b *client.Batch, key proto.Key, desc *proto.RangeDescriptor) {
	b.Del(key)
}

// splitRangeAddressing creates (or overwrites if necessary) the meta1
// and meta2 range addressing records for the left and right ranges
// caused by a split.
func splitRangeAddressing(b *client.Batch, left, right *proto.RangeDescriptor) error {
	var err error
	if err = rangeAddressing(b, left, putMeta); err != nil {
		return err
	}
	return rangeAddressing(b, right, putMeta)
}

// mergeRangeAddressing removes subsumed meta1 and meta2 range
// addressing records caused by merging and updates the records for
// the new merged range. Left is the range descriptor for the "left"
// range before merging and merged describes the left to right merge.
func mergeRangeAddressing(b *client.Batch, left, merged *proto.RangeDescriptor) error {
	var err error
	if err = rangeAddressing(b, left, delMeta); err != nil {
		return err
	}
	return rangeAddressing(b, merged, putMeta)
}

// updateRangeAddressing overwrites the meta1 and meta2 range addressing
// records for the descriptor.
func updateRangeAddressing(b *client.Batch, desc *proto.RangeDescriptor) error {
	return rangeAddressing(b, desc, putMeta)
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
func rangeAddressing(b *client.Batch, desc *proto.RangeDescriptor, action metaAction) error {
	// 1. handle illegal case of start or end key being meta1.
	if bytes.HasPrefix(desc.EndKey, keys.Meta1Prefix) ||
		bytes.HasPrefix(desc.StartKey, keys.Meta1Prefix) {
		return util.Errorf("meta1 addressing records cannot be split: %+v", desc)
	}
	// 2. the case of the range ending with a meta2 prefix. This means
	// the range is full of meta2. We must update the relevant meta1
	// entry pointing to the end of this range.
	if bytes.HasPrefix(desc.EndKey, keys.Meta2Prefix) {
		action(b, keys.RangeMetaKey(desc.EndKey), desc)
	} else {
		// 3. the range ends with a normal user key, so we must update the
		// relevant meta2 entry pointing to the end of this range.
		action(b, keys.MakeKey(keys.Meta2Prefix, desc.EndKey), desc)
		// 3a. the range starts with KeyMin or a meta2 addressing record,
		// update the meta1 entry for KeyMax.
		if bytes.Equal(desc.StartKey, proto.KeyMin) ||
			bytes.HasPrefix(desc.StartKey, keys.Meta2Prefix) {
			action(b, keys.MakeKey(keys.Meta1Prefix, proto.KeyMax), desc)
		}
	}
	return nil
}
