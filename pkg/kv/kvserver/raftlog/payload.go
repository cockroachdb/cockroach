// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftlog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// RaftCmdToPayload encodes the provided command into a slice.
func RaftCmdToPayload(
	_ context.Context, command *kvserverpb.RaftCommand, idKey kvserverbase.CmdIDKey,
) ([]byte, error) {
	// Determine the encoding style for the Raft command.
	prefix := true
	entryEncoding := EntryEncodingStandardWithoutAC
	if crt := command.ReplicatedEvalResult.ChangeReplicas; crt != nil {
		// EndTxnRequest with a ChangeReplicasTrigger is special because Raft
		// needs to understand it; it cannot simply be an opaque command. To
		// permit this, the command is proposed by the proposal buffer using
		// ProposeConfChange. For that reason, we also don't need a Raft command
		// prefix because the command ID is stored in a field in
		// raft.ConfChange.
		prefix = false
	} else if command.ReplicatedEvalResult.AddSSTable != nil {
		entryEncoding = EntryEncodingSideloadedWithoutAC

		if command.ReplicatedEvalResult.AddSSTable.Data == nil {
			return nil, errors.Errorf("cannot sideload empty SSTable")
		}
	}

	// Create encoding buffer.
	preLen := 0
	if prefix {
		preLen = RaftCommandPrefixLen
	}
	cmdLen := command.Size()
	// Allocate the data slice with enough capacity to eventually hold the two
	// "footers" that are filled later.
	needed := preLen + cmdLen + kvserverpb.MaxRaftCommandFooterSize()
	data := make([]byte, preLen, needed)
	// Encode prefix with command ID, if necessary.
	if prefix {
		EncodeRaftCommandPrefix(data, entryEncoding, idKey)
	}
	// Encode body of command.
	data = data[:preLen+cmdLen]
	if _, err := protoutil.MarshalTo(command, data[preLen:]); err != nil {
		return nil, err
	}
	return data, nil
}
