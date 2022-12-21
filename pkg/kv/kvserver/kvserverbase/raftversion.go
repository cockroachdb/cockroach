// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverbase

import "fmt"

// RaftCommandEncodingVersion versions CockroachDB's raft entries.
// A raftpb.Entry's RaftCommandEncodingVersion is a function of the
// Entry's raftpb.EntryType and, in some cases, the first byte of the
// Entry's Data payload.
//
// TODO(tbg): it versions them, but there are really different types
// and none of them is deprecated, so consider renaming this.
type RaftCommandEncodingVersion byte

const (
	// RaftVersionStandard is the default encoding for a CockroachDB raft log
	// entry.
	//
	// This is a raftpb.Entry of type EntryNormal whose Data slice is either
	// empty or whose first byte matches RaftVersionStandard. The subsequent
	// eight bytes represent a CmdIDKey. The remaining bytes represent a
	// kvserverpb.RaftCommand.
	RaftVersionStandard RaftCommandEncodingVersion = 0
	// RaftVersionSideloaded indicates a proposal representing the result of a
	// roachpb.AddSSTableRequest for which the payload (the SST) is stored outside
	// the storage engine to improve storage performance.
	//
	// This is a raftpb.Entry of type EntryNormal whose data slice is either empty
	// or whose first byte matches RaftVersionSideloaded. The remaining bytes
	// represent a kvserverpb.RaftCommand whose kvserverpb.ReplicatedEvalResult
	// holds a nontrival kvserverpb.ReplicatedEvalResult_AddSSTable, the Data
	// field of which is an SST to be ingested (and which is present in memory but
	// made durable via direct storage on the filesystem, bypassing the storage
	// engine).
	RaftVersionSideloaded RaftCommandEncodingVersion = 1
	// RaftVersionEmptyEntry is an empty entry. These are used by raft after
	// leader election. Since they hold no data, there is nothing in them to
	// decode.
	RaftVersionEmptyEntry RaftCommandEncodingVersion = 253
	// RaftVersionConfChange is a raftpb.Entry whose raftpb.EntryType is
	// raftpb.EntryConfChange. The Entry's Data field holds a raftpb.ConfChange
	// whose Context field is a kvserverpb.ConfChangeContext whose Payload is a
	// kvserverpb.RaftCommand. In particular, the CmdIDKey requires a round of
	// protobuf unmarshaling.
	RaftVersionConfChange RaftCommandEncodingVersion = 254
	// RaftVersionConfChangeV2 is analogous to RaftVersionConfChange, with
	// the replacements raftpb.EntryConfChange{,V2} and raftpb.ConfChange{,V2}
	// applied.
	RaftVersionConfChangeV2 RaftCommandEncodingVersion = 255
)

// TODO(tbg): when we have a good library for encoding entries, these should
// no longer be exported.
const (
	// RaftCommandIDLen is the length of a command ID.
	RaftCommandIDLen = 8
	// RaftCommandPrefixLen is the length of the prefix of raft entries that
	// use the RaftVersionStandard or RaftVersionSideloaded encodings. The
	// bytes after the prefix represent the kvserverpb.RaftCommand.
	//
	RaftCommandPrefixLen = 1 + RaftCommandIDLen
)

// EncodeRaftCommand encodes a raft command (including the versioning prefix).
func EncodeRaftCommand(
	version RaftCommandEncodingVersion, commandID CmdIDKey, command []byte,
) []byte {
	b := make([]byte, RaftCommandPrefixLen+len(command))
	EncodeRaftCommandPrefix(b[:RaftCommandPrefixLen], version, commandID)
	copy(b[RaftCommandPrefixLen:], command)
	return b
}

// EncodeRaftCommandPrefix encodes the versioning prefix for a Raft command.
func EncodeRaftCommandPrefix(b []byte, version RaftCommandEncodingVersion, commandID CmdIDKey) {
	if len(commandID) != RaftCommandIDLen {
		panic(fmt.Sprintf("invalid command ID length; %d != %d", len(commandID), RaftCommandIDLen))
	}
	if len(b) != RaftCommandPrefixLen {
		panic(fmt.Sprintf("invalid command prefix length; %d != %d", len(b), RaftCommandPrefixLen))
	}
	b[0] = byte(version)
	copy(b[1:], commandID)
}
