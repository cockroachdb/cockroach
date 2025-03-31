// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftstoretoy/rscodec"

// WAGNodeType identifies the type of operation in the Write-Ahead Graph.
type WAGNodeType int

const (
	// WAGNodeSnap represents a snapshot operation
	WAGNodeSnap WAGNodeType = iota
	// WAGNodeSplit represents a range split operation
	WAGNodeSplit
	// WAGNodeMerge represents a range merge operation
	WAGNodeMerge
	// WAGNodeDestroy represents a range destruction operation
	WAGNodeDestroy
)

// LogPosition identifies a specific position in a Raft log.
type LogPosition struct {
	rscodec.LogID
	Index RaftIndex
}

// WAGDependency represents a dependency edge in the Write-Ahead Graph.
type WAGDependency struct {
	NodeID   int64       // Unique identifier for the WAG node
	Position LogPosition // Log position this dependency refers to
	NodeType WAGNodeType // Type of the dependent node
}

// WAGNode represents a node in the Write-Ahead Graph. Each node represents
// a lifecycle operation that may need to be replayed after a crash.
type WAGNode interface {
	// Type returns the type of operation this node represents
	Type() WAGNodeType
	// Dependencies returns the list of other nodes this node depends on
	Dependencies() []WAGDependency
	// LogPosition returns where this operation is anchored in the Raft log
	LogPosition() LogPosition
}

// Concrete WAG node implementations will go here
