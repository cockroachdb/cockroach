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
	"github.com/cockroachdb/cockroach/util"
)

// SetupRangeTree creates a new range tree.  This should only be called as part of
// store.BootstrapRange.
func (s *Store) SetupRangeTree(batch engine.Engine, ms *engine.MVCCStats,
	timestamp proto.Timestamp, raftID int64) (*proto.RangeTree, *proto.RangeTreeNode, error) {
	var previousTree proto.RangeTree
	ok, err := engine.MVCCGetProto(batch, engine.KeyRangeTreeRoot, timestamp, nil, &previousTree)
	if err != nil {
		return nil, nil, err
	}
	if ok {
		return nil, nil, util.Errorf("range tree already exists")
	}

	tree := &proto.RangeTree{
		RootID: raftID,
	}

	node := &proto.RangeTreeNode{
		RaftID: raftID,
		Black:  true,
	}

	if err = engine.MVCCPutProto(batch, ms, engine.RangeTreeNodeKey(raftID), timestamp, nil, node); err != nil {
		return nil, nil, err
	}

	if err = engine.MVCCPutProto(batch, ms, engine.KeyRangeTreeRoot, timestamp, nil, tree); err != nil {
		return nil, nil, err
	}

	return tree, node, nil
}
