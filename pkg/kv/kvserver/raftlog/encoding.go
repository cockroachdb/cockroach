// Copyright 2022 The Cockroach Authors.
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
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowconnectedstream"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// EntryEncoding enumerates the encodings used in CockroachDB for raftpb.Entry's
// Data slice.
//
// A raftpb.Entry's EntryEncoding is determined by the Entry's raftpb.EntryType
// and, in some cases, the first byte of the Entry's Data payload. This is a
// decoded type and not on the wire.
type EntryEncoding byte

const (
	// EntryEncodingEmpty is an empty entry. These are used by raft after
	// leader election. Since they hold no data, there is nothing in them to
	// decode.
	EntryEncodingEmpty EntryEncoding = iota
	// EntryEncodingStandardWithAC is the default encoding for a CockroachDB
	// raft log entry.
	//
	// This is a raftpb.Entry of type EntryNormal whose Data slice has a first byte
	// that matches entryEncodingStandardWithACPrefixByte.
	// The subsequent eight bytes represent a CmdIDKey. The remaining bytes
	// represent a kvserverpb.RaftCommand that also includes data used for
	// below-raft admission control (Admission{Priority,CreateTime,OriginNode}).
	EntryEncodingStandardWithAC
	// EntryEncodingSideloadedWithAC indicates a proposal representing the
	// result of a kvpb.AddSSTableRequest for which the payload (the SST) is
	// stored outside the storage engine to improve storage performance.
	//
	// This is a raftpb.Entry of type EntryNormal whose data slice has a first
	// byte == entryEncodingSideloadedWithACPrefixByte. The
	// subsequent eight bytes represent a CmdIDKey. The remaining bytes
	// represent a kvserverpb.RaftCommand whose kvserverpb.ReplicatedEvalResult
	// holds a nontrival kvserverpb.ReplicatedEvalResult_AddSSTable, the Data
	// field of which is an SST to be ingested (and which is present in memory
	// but made durable via direct storage on the filesystem, bypassing the
	// storage engine). Admission{Priority,CreateTime,OriginNode} in the
	// kvserverpb.RaftCommand are non-empty, data used for below-raft admission
	// control.
	EntryEncodingSideloadedWithAC
	// EntryEncodingStandardWithoutAC is like EntryEncodingStandardWithAC but
	// without the data for below-raft admission control.
	EntryEncodingStandardWithoutAC
	// EntryEncodingSideloadedWithoutAC is like EntryEncodingStandardWithoutAC
	// but without below-raft admission metadata.
	EntryEncodingSideloadedWithoutAC
	// EntryEncodingRaftConfChange is a raftpb.Entry whose raftpb.EntryType is
	// raftpb.EntryConfChange. The Entry's Data field holds a raftpb.ConfChange
	// whose Context field is a kvserverpb.ConfChangeContext whose Payload is a
	// kvserverpb.RaftCommand. In particular, the CmdIDKey requires a round of
	// protobuf unmarshaling.
	EntryEncodingRaftConfChange
	// EntryEncodingRaftConfChangeV2 is analogous to
	// EntryEncodingRaftConfChange, with the replacements
	// raftpb.EntryConfChange{,V2} and raftpb.ConfChange{,V2} applied.
	EntryEncodingRaftConfChangeV2

	// For RACv2

	// EntryEncodingStandardWithRaftPriority is analogous to
	// EntryEncodingStandardWithAC, but has the most significant bits in the
	// first byte containing the raft priority. This is the priority that is
	// also encoded in the kvflowcontrolpb.RaftAdmissionMeta, but is cheap to
	// decode.
	EntryEncodingStandardWithRaftPriority
	// EntryEncodingSideloadedWithRaftPriority ...
	EntryEncodingSideloadedWithRaftPriority
)

// IsSideloaded returns true if the encoding is
// EntryEncodingSideloadedWith{,out}AC.
func (enc EntryEncoding) IsSideloaded() bool {
	return enc == EntryEncodingSideloadedWithAC || enc == EntryEncodingSideloadedWithoutAC ||
		enc == EntryEncodingSideloadedWithRaftPriority
}

// UsesAdmissionControl returns true if the encoding is
// EntryEncoding{Standard,Sideloaded}WithAC.
func (enc EntryEncoding) UsesAdmissionControl() bool {
	return enc == EntryEncodingStandardWithAC || enc == EntryEncodingSideloadedWithAC ||
		enc == EntryEncodingStandardWithRaftPriority || enc == EntryEncodingSideloadedWithRaftPriority
}

// prefixByte returns the prefix byte used during encoding, applicable only to
// EntryEncoding{Standard,Sideloaded}With{,out}AC.
func (enc EntryEncoding) prefixByte(pri kvflowconnectedstream.RaftPriority) byte {
	if pri > 3 {
		panic("")
	}
	switch enc {
	case EntryEncodingStandardWithAC:
		return entryEncodingStandardWithACPrefixByte
	case EntryEncodingSideloadedWithAC:
		return entryEncodingSideloadedWithACPrefixByte
	case EntryEncodingStandardWithoutAC:
		return entryEncodingStandardWithoutACPrefixByte
	case EntryEncodingSideloadedWithoutAC:
		return entryEncodingSideloadedWithoutACPrefixByte
	case EntryEncodingStandardWithRaftPriority:
		return entryEncodingStandardWithRaftPriorityPrefixByte | (byte(pri) << 6)
	case EntryEncodingSideloadedWithRaftPriority:
		return entryEncodingSideloadedWithRaftPriorityPrefixByte | (byte(pri) << 6)
	default:
		panic(fmt.Sprintf("invalid encoding: %v has no prefix byte", enc))
	}
}

const (
	// entryEncodingStandardWithRaftPriorityPrefixByte is the first byte of a
	// raftpb.Entry's Data slice for an Entry of encoding
	// EntryEncodingStandardWithRaftPriority, after applying encodingMask.
	entryEncodingStandardWithRaftPriorityPrefixByte = byte(4) // 0b00000100
	// entryEncodingSideloadedWithRaftPriorityPrefixByte is the first byte of a
	// raftpb.Entry's Data slice for an Entry of encoding
	// EntryEncodingSideloadedWithRaftPriority, after applying encodingMask.
	entryEncodingSideloadedWithRaftPriorityPrefixByte = byte(5) // 0b00000101
	// entryEncodingStandardWithACPrefixByte is the first byte of a
	// raftpb.Entry's Data slice for an Entry of encoding
	// EntryEncodingStandardWithAC.
	entryEncodingStandardWithACPrefixByte = byte(2) // 0b00000010
	// entryEncodingSideloadedWithACPrefixByte is the first byte of a
	// raftpb.Entry's Data slice for an Entry of encoding
	// EntryEncodingSideloadedWithAC.
	entryEncodingSideloadedWithACPrefixByte = byte(3) // 0b00000011
	// entryEncodingStandardWithoutACPrefixByte is the first byte of a
	// raftpb.Entry's Data slice for an Entry of encoding
	// EntryEncodingStandardWithoutAC.
	entryEncodingStandardWithoutACPrefixByte = byte(0) // 0b00000000
	// entryEncodingSideloadedWithoutACPrefixByte is the first byte of a
	// raftpb.Entry's Data slice for an Entry of encoding
	// EntryEncodingSideloadedWithoutAC.
	entryEncodingSideloadedWithoutACPrefixByte = byte(1) // 0b00000001
)

const (
	// RaftCommandIDLen is the length of a command ID.
	RaftCommandIDLen = 8
	// RaftCommandPrefixLen is the length of the prefix of raft entries that use
	// the EntryEncoding{Standard,Sideloaded}With{,out}AC encodings. The bytes
	// after the prefix represent the kvserverpb.RaftCommand.
	RaftCommandPrefixLen = 1 + RaftCommandIDLen
)

// EncodeCommandBytes encodes a marshaled kvserverpb.RaftCommand using
// the given encoding (one of EntryEncoding{Standard,Sideloaded}With{,out}AC).
//
// This is 1 byte for the EntryEncoding + 8 bytes for the CmdIDKey + command.
//
// If EntryEncoding is one of the WithRaftPriority encodings, the pri parameter
// is used.
func EncodeCommandBytes(
	enc EntryEncoding,
	commandID kvserverbase.CmdIDKey,
	command []byte,
	pri kvflowconnectedstream.RaftPriority,
) []byte {
	b := make([]byte, RaftCommandPrefixLen+len(command))
	EncodeRaftCommandPrefix(b[:RaftCommandPrefixLen], enc, commandID, pri)
	copy(b[RaftCommandPrefixLen:], command)
	return b
}

// EncodeRaftCommandPrefix encodes the prefix for a Raft command, using the
// given encoding (one of EntryEncoding{Standard,Sideloaded}With{,out}AC).
func EncodeRaftCommandPrefix(
	b []byte,
	enc EntryEncoding,
	commandID kvserverbase.CmdIDKey,
	pri kvflowconnectedstream.RaftPriority,
) {
	if len(commandID) != RaftCommandIDLen {
		panic(fmt.Sprintf("invalid command ID length; %d != %d", len(commandID), RaftCommandIDLen))
	}
	if len(b) != RaftCommandPrefixLen {
		panic(fmt.Sprintf("invalid command prefix length; %d != %d", len(b), RaftCommandPrefixLen))
	}
	b[0] = enc.prefixByte(pri)
	copy(b[1:], commandID)
}

// DecodeRaftAdmissionMeta decodes admission control metadata from a
// raftpb.Entry.Data. Expects an EntryEncoding{Standard,Sideloaded}WithAC
// encoding.
// 
// TODO(rac-v2): Unit test this function and the corresponding construction.
func DecodeRaftAdmissionMeta(data []byte) (kvflowcontrolpb.RaftAdmissionMeta, error) {
	prefix := data[0] & encodingMask
	if !(prefix == entryEncodingStandardWithACPrefixByte || prefix == entryEncodingSideloadedWithACPrefixByte ||
		prefix == entryEncodingStandardWithRaftPriorityPrefixByte ||
		prefix == entryEncodingSideloadedWithRaftPriorityPrefixByte) {
		panic(fmt.Sprintf("invalid encoding: prefix %v", prefix))
	}

	// TODO(irfansharif): If the decoding overhead is noticeable, we can write a
	// custom decoder and rely on the encoding for raft admission data being
	// present at the start of the marshaled raft command. This could speed it
	// up slightly.
	var raftAdmissionMeta kvflowcontrolpb.RaftAdmissionMeta
	if err := protoutil.Unmarshal(data[RaftCommandPrefixLen:], &raftAdmissionMeta); err != nil {
		return kvflowcontrolpb.RaftAdmissionMeta{}, err
	}
	pri := data[0] & priMask
	switch prefix {
	case entryEncodingStandardWithRaftPriorityPrefixByte, entryEncodingSideloadedWithRaftPriorityPrefixByte:
		if kvflowconnectedstream.RaftPriority(pri) !=
			kvflowconnectedstream.RaftPriority(raftAdmissionMeta.AdmissionPriority) {
			panic("")
		}
	}
	return raftAdmissionMeta, nil
}

// MakeCmdIDKey populates a random CmdIDKey.
func MakeCmdIDKey() kvserverbase.CmdIDKey {
	idKeyBuf := make([]byte, 0, RaftCommandIDLen)
	idKeyBuf = encoding.EncodeUint64Ascending(idKeyBuf, uint64(rand.Int63()))
	return kvserverbase.CmdIDKey(idKeyBuf)
}
