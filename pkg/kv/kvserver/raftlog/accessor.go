// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftlog

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

// Accessor provides access to the raft log storage.
type Accessor interface {
	GetCompactedIndex() kvpb.RaftIndex
	GetTerm(index kvpb.RaftIndex) (kvpb.RaftTerm, error)
	// LogEngine returns the engine that stores the raft log.
	LogEngine() storage.Engine
}

// MockAccessor is a mock implementation of Accessor for testing.
type MockAccessor struct {
	CompactedIndex kvpb.RaftIndex
	Term           kvpb.RaftTerm
	Engine         storage.Engine
}

func (m *MockAccessor) GetCompactedIndex() kvpb.RaftIndex {
	return m.CompactedIndex
}

func (m *MockAccessor) GetTerm(kvpb.RaftIndex) (kvpb.RaftTerm, error) {
	return m.Term, nil
}

func (m *MockAccessor) LogEngine() storage.Engine {
	if m.Engine != nil {
		return m.Engine
	}
	panic("LogEngine not configured")
}
