// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowconnectedstream

import (
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

type raftEventImpl raft.Ready

func MakeRaftEvent(ready *raft.Ready) RaftEvent {
	return (*raftEventImpl)(ready)
}

func (e *raftEventImpl) Ready() Ready {
	if e != nil {
		return e
	}
	return nil
}

func (e *raftEventImpl) GetEntries() []raftpb.Entry {
	return (*raft.Ready)(e).Entries
}
