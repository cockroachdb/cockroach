// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

// replicaRaftLogAccessor implements raftlog.Accessor.
type replicaRaftLogAccessor struct {
	r *Replica
}

var _ raftlog.Accessor = (*replicaRaftLogAccessor)(nil)

func (a *replicaRaftLogAccessor) GetCompactedIndex() kvpb.RaftIndex {
	return a.r.GetCompactedIndex()
}

func (a *replicaRaftLogAccessor) GetTerm(index kvpb.RaftIndex) (kvpb.RaftTerm, error) {
	return a.r.GetTerm(index)
}

func (a *replicaRaftLogAccessor) LogEngine() storage.Engine {
	return a.r.LogEngine()
}
