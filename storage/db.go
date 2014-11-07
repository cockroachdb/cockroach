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

// UpdateRangeAddressing updates the range addressing metadata for the
// range specified by desc.
func UpdateRangeAddressing(db *client.KV, desc *proto.RangeDescriptor) error {
	// First, the case of the range ending with a meta2 prefix. This
	// means the range is full of meta2. We must update the relevant
	// meta1 entry pointing to the end of this range.
	if bytes.HasPrefix(desc.EndKey, engine.KeyMeta2Prefix) {
		if err := db.PreparePutProto(engine.RangeMetaKey(desc.EndKey), desc); err != nil {
			return err
		}
	} else {
		// In this case, the range ends with a normal user key, so we must
		// update the relevant meta2 entry pointing to the end of this range.
		if err := db.PreparePutProto(engine.MakeKey(engine.KeyMeta2Prefix, desc.EndKey), desc); err != nil {
			return err
		}
		// If the range starts with KeyMin, we update the appropriate
		// meta1 entry.
		if bytes.Equal(desc.StartKey, engine.KeyMin) {
			if bytes.HasPrefix(desc.EndKey, engine.KeyMeta2Prefix) {
				if err := db.PreparePutProto(engine.RangeMetaKey(desc.EndKey), desc); err != nil {
					return err
				}
			} else if !bytes.HasPrefix(desc.EndKey, engine.KeyMetaPrefix) {
				if err := db.PreparePutProto(engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax), desc); err != nil {
					return err
				}
			} else {
				return util.Errorf("meta1 addressing records cannot be split: %+v", desc)
			}
		} else if bytes.HasPrefix(desc.StartKey, engine.KeyMeta2Prefix) {
			// If the range contains the final set of meta2 addressing
			// records, we update the meta1 entry pointing to KeyMax.
			if err := db.PreparePutProto(engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax), desc); err != nil {
				return err
			}
		} else if bytes.HasPrefix(desc.StartKey, engine.KeyMeta1Prefix) {
			return util.Errorf("meta1 addressing records cannot be split: %+v", desc)
		}
	}
	return nil
}
