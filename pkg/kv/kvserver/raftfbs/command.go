// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftfbs

import flatbuffers "github.com/google/flatbuffers/go"

// BuildCommand builds a Command message via the Builder and returns its offset.
func BuildCommand(b *flatbuffers.Builder, cmdID []byte, raftCmdPb []byte) flatbuffers.UOffsetT {
	id := b.CreateByteVector(cmdID)
	cmdPB := b.CreateByteVector(raftCmdPb)

	CommandStart(b)
	CommandAddId(b, id)
	CommandAddRaftCommandPb(b, cmdPB)
	return CommandEnd(b)
}
