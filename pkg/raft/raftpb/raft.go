// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftpb

import "github.com/cockroachdb/redact"

// PeerID is a custom type for peer IDs in a raft group.
type PeerID uint64

// SafeValue implements the redact.SafeValue interface.
func (p PeerID) SafeValue() {}

// Epoch is an epoch in the Store Liveness fabric, referencing an uninterrupted
// period of support from one store to another.
type Epoch int64

// SafeValue implements the redact.SafeValue interface.
func (e Epoch) SafeValue() {}

// Priority is used for replication admission control v2 (RAC2), where Raft
// tracks admitted values per priority, and provides liveness by pinging when
// the admitted values are lagging.
type Priority uint8

const (
	LowPri Priority = iota
	NormalPri
	AboveNormalPri
	HighPri
	NumPriorities
)

func (p Priority) String() string {
	return redact.StringWithoutMarkers(p)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (p Priority) SafeFormat(w redact.SafePrinter, _ rune) {
	switch p {
	case LowPri:
		w.Printf("LowPri")
	case NormalPri:
		w.Printf("NormalPri")
	case AboveNormalPri:
		w.Printf("AboveNormalPri")
	case HighPri:
		w.Printf("HighPri")
	default:
		panic("invalid raft priority")
	}
}
