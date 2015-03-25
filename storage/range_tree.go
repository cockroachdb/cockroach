// Copyright 2015 The Cockroach Authors.
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
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package storage

import (
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
)

// SetupRangeTree creates a new range tree.  This should only be called as part of
// store.BootstrapRange.
func SetupRangeTree(batch engine.Engine, ms *engine.MVCCStats,
	timestamp proto.Timestamp, startKey proto.Key) error {

	tree := &proto.RangeTree{
		RootKey: startKey,
	}

	node := &proto.RangeTreeNode{
		Key:   startKey,
		Black: true,
	}

	if err := engine.MVCCPutProto(batch, ms, engine.RangeTreeNodeKey(startKey), timestamp, nil, node); err != nil {
		return err
	}

	if err := engine.MVCCPutProto(batch, ms, engine.KeyRangeTreeRoot, timestamp, nil, tree); err != nil {
		return err
	}

	return nil
}
