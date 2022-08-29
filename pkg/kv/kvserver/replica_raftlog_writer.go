// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type replicaRaftLog Replica

var _ raftlog.RaftRange = &replicaRaftLog{}

func (r *replicaRaftLog) StateLoader() stateloader.StateLoader {
	return r.raftMu.logWriterStateLoader
}

func (r *replicaRaftLog) MaybeSideloadEntries(
	ctx context.Context, entries []raftpb.Entry,
) (_ []raftpb.Entry, sideloadedEntriesSize int64, _ error) {
	thinEntries, _, sideloadedEntriesSize, _, err := maybeSideloadEntriesImpl(ctx, entries, r.raftMu.sideloaded)
	return thinEntries, sideloadedEntriesSize, err
}

func (r *replicaRaftLog) GetRaftLogMetadata() raftlog.RaftLogMetadata {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return raftlog.RaftLogMetadata{
		LastIndex: r.mu.lastIndex,
		LastTerm:  r.mu.lastTerm,
		LogSize:   r.mu.raftLogSize,
	}
}

func (r *replicaRaftLog) LogStableTo(rd raft.Ready, meta raftlog.RaftLogMetadata) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.lastIndex = meta.LastIndex
	r.mu.lastTerm = meta.LastTerm
	r.mu.raftLogSize = meta.LogSize
	r.raftMu.logWriterReady = rd
	r.store.enqueueRaftUpdateCheck(r.RangeID)
}
