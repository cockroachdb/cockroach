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

// PeerID is a custom type for peer IDs in a raft group.
type PeerID uint64

// SafeValue implements the redact.SafeValue interface.
func (p PeerID) SafeValue() {}
