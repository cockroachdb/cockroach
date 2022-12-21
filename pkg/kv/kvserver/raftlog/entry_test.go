// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package raftlog

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestLoadInvalidEntry(t *testing.T) {
	invalidEnt := raftpb.Entry{
		Term:  1,
		Index: 1,
		Data: EncodeRaftCommand(
			// It would be nice to have an "even more invalid" command here but it
			// turns out that DecodeRaftCommand "handles" errors via panic().
			RaftVersionStandardPrefixByte, "foobarzz", []byte("definitely not a protobuf"),
		),
	}
	ent, err := NewEntry(invalidEnt)
	require.Error(t, err) // specific error doesn't matter
	require.Zero(t, ent)
}
