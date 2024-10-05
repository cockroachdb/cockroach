// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftpb

// PeerID is a custom type for peer IDs in a raft group.
type PeerID uint64

// SafeValue implements the redact.SafeValue interface.
func (p PeerID) SafeValue() {}
