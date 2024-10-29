// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftlog

import (
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// EntryEncoding enumerates the encodings used in CockroachDB for raftpb.Entry's
// Data slice.
//
// A raftpb.Entry's EntryEncoding is determined by the Entry's raftpb.EntryType
// and, in some cases, the first byte of the Entry's Data payload. This is a
// decoded type and not part of the wire protocol.
type EntryEncoding byte

const (
	// EntryEncodingEmpty is an empty entry. These are used by raft after
	// leader election. Since they hold no data, there is nothing in them to
	// decode.
	EntryEncodingEmpty EntryEncoding = iota
	// EntryEncodingStandardWithAC is the encoding for a CockroachDB raft log
	// entry in replication admission control v1.
	//
	// This is a raftpb.Entry of type raftpb.EntryNormal whose Data slice has a
	// first byte that matches entryEncodingStandardWithACPrefixByte.
	// The subsequent eight bytes represent a CmdIDKey. The remaining bytes
	// represent a kvserverpb.RaftCommand that also includes metadata used for
	// below-raft admission control (Admission{Priority,CreateTime,OriginNode}),
	// which is represented using the RaftAdmissionMeta proto.
	EntryEncodingStandardWithAC
	// EntryEncodingSideloadedWithAC indicates a proposal representing the
	// result of a kvpb.AddSSTableRequest for which the payload (the SST) is
	// stored outside the storage engine to improve storage performance, and is
	// encoded for replication admission control v1.
	//
	// This is a raftpb.Entry of type EntryNormal whose data slice has a
	// first byte == entryEncodingSideloadedWithACPrefixByte. The subsequent
	// eight bytes represent a CmdIDKey. The remaining bytes
	// represent a kvserverpb.RaftCommand whose kvserverpb.ReplicatedEvalResult
	// holds a nontrival kvserverpb.ReplicatedEvalResult_AddSSTable, the Data
	// field of which is an SST to be ingested (and which is present in memory
	// but made durable via direct storage on the filesystem, bypassing the
	// storage engine). Admission{Priority,CreateTime,OriginNode} in the
	// kvserverpb.RaftCommand are non-empty (represented using the
	// RaftAdmissionMeta proto), metadata used for below-raft admission control.
	EntryEncodingSideloadedWithAC
	// EntryEncodingStandardWithoutAC is like EntryEncodingStandardWithAC but
	// without the metadata for below-raft admission control.
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
	// EntryEncodingStandardWithACAndPriority is analogous to
	// EntryEncodingStandardWithAC, but additionally has the most significant
	// bits in the first byte containing the raftpb.Priority. This is the
	// priority that is also encoded in the kvflowcontrolpb.RaftAdmissionMeta,
	// but is cheap to decode. This encoding is for replication admission
	// control v2.
	EntryEncodingStandardWithACAndPriority
	// EntryEncodingSideloadedWithACAndPriority is analogous to
	// EntryEncodingSideloadedWithAC, but additionally has the most significant
	// bits in the first byte containing the raftpb.Priority. This is the
	// priority that is also encoded in the kvflowcontrolpb.RaftAdmissionMeta,
	// but is cheap to decode. This encoding is for replication admission
	// control v2.
	EntryEncodingSideloadedWithACAndPriority
)

// IsSideloaded returns true if the encoding is
// EntryEncodingSideloadedWith{,out}AC.
func (enc EntryEncoding) IsSideloaded() bool {
	return enc == EntryEncodingSideloadedWithAC || enc == EntryEncodingSideloadedWithoutAC ||
		enc == EntryEncodingSideloadedWithACAndPriority
}

// UsesAdmissionControl returns true if the encoding is
// EntryEncoding{Standard,Sideloaded}WithAC.
func (enc EntryEncoding) UsesAdmissionControl() bool {
	return enc == EntryEncodingStandardWithAC || enc == EntryEncodingSideloadedWithAC ||
		enc == EntryEncodingStandardWithACAndPriority ||
		enc == EntryEncodingSideloadedWithACAndPriority
}

// encodingMask is used to encode the encoding type in the lower 6 bits of the
// first byte in the entry encoding.
const encodingMask byte = 0x3F

// priMask is used to encode the raftpb.Priority of the command in the highest
// 2 bits of the first byte in the entry encoding.
const priMask byte = 0xC0

const priShift = 6

// getPriority returns the raftpb.Priority, given the first byte of the entry
// encoding.
//
// REQUIRES: b is one of the WithACAndPriority encodings.
func getPriority(b byte) raftpb.Priority {
	if buildutil.CrdbTestBuild {
		encodingType := b & encodingMask
		switch encodingType {
		case entryEncodingStandardWithACAndPriorityPrefixByte, entryEncodingSideloadedWithACAndPriorityPrefixByte:
		default:
			panic(errors.AssertionFailedf("unexpected type %d", encodingType))
		}
	}
	return raftpb.Priority((b & priMask) >> priShift)
}

// prefixByte returns the prefix byte used during encoding, applicable only to
// EntryEncoding{Standard,Sideloaded}With{,out}AC{AndPriority}. pri is used
// only for the WithACAndPriority encodings.
func (enc EntryEncoding) prefixByte(pri raftpb.Priority) byte {
	if buildutil.CrdbTestBuild {
		if pri >= 4 {
			panic(errors.AssertionFailedf("pri is out of expected bounds %d", pri))
		}
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
	case EntryEncodingStandardWithACAndPriority:
		return entryEncodingStandardWithACAndPriorityPrefixByte | (byte(pri) << priShift)
	case EntryEncodingSideloadedWithACAndPriority:
		return entryEncodingSideloadedWithACAndPriorityPrefixByte | (byte(pri) << priShift)
	default:
		panic(fmt.Sprintf("invalid encoding: %v has no prefix byte", enc))
	}
}

const (
	// entryEncodingStandardWithACAndPriorityPrefixByte is the first byte of a
	// raftpb.Entry's Data slice for an Entry of encoding
	// EntryEncodingStandardWithACAndPriority, after applying encodingMask.
	entryEncodingStandardWithACAndPriorityPrefixByte = byte(4) // 0b00000100
	// entryEncodingSideloadedWithACAndPriorityPrefixByte is the first byte of a
	// raftpb.Entry's Data slice for an Entry of encoding
	// EntryEncodingSideloadedWithACAndPriority, after applying encodingMask.
	entryEncodingSideloadedWithACAndPriorityPrefixByte = byte(5) // 0b00000101
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
// the given encoding (one of EntryEncoding{Standard,Sideloaded}With{,out}AC{AndPriority}).
//
// The overall encoding uses 1 byte for the EntryEncoding + 8 bytes for the
// CmdIDKey + command.
//
// If EntryEncoding is one of the WithACAndPriority encodings, the pri
// parameter is used.
func EncodeCommandBytes(
	enc EntryEncoding, commandID kvserverbase.CmdIDKey, command []byte, pri raftpb.Priority,
) []byte {
	b := make([]byte, RaftCommandPrefixLen+len(command))
	EncodeRaftCommandPrefix(b[:RaftCommandPrefixLen], enc, commandID, pri)
	copy(b[RaftCommandPrefixLen:], command)
	return b
}

// EncodeRaftCommandPrefix encodes the prefix for a Raft command, using the
// given encoding (one of
// EntryEncoding{Standard,Sideloaded}With{,out}AC{AndPriority}). If
// EntryEncoding is one of the WithACAndPriority encodings, the pri parameter
// is used.
func EncodeRaftCommandPrefix(
	b []byte, enc EntryEncoding, commandID kvserverbase.CmdIDKey, pri raftpb.Priority,
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
func DecodeRaftAdmissionMeta(data []byte) (kvflowcontrolpb.RaftAdmissionMeta, error) {
	prefix := data[0] & encodingMask
	if !(prefix == entryEncodingStandardWithACPrefixByte ||
		prefix == entryEncodingSideloadedWithACPrefixByte ||
		prefix == entryEncodingStandardWithACAndPriorityPrefixByte ||
		prefix == entryEncodingSideloadedWithACAndPriorityPrefixByte) {
		panic(fmt.Sprintf("invalid encoding: prefix %v", prefix))
	}

	// TODO(irfansharif): If the decoding overhead is noticeable, we can write a
	// custom decoder and rely on the encoding for raft admission data being
	// present at the start of the marshaled raft command. This could speed it
	// up slightly.
	var raftAdmissionMeta kvflowcontrolpb.RaftAdmissionMeta
	// NB: we don't use protoutil.Unmarshal to avoid passing raftAdmissionMeta
	// through an interface, which would cause a heap allocation.
	if err := raftAdmissionMeta.Unmarshal(data[RaftCommandPrefixLen:]); err != nil {
		return kvflowcontrolpb.RaftAdmissionMeta{}, err
	}
	if buildutil.CrdbTestBuild {
		switch prefix {
		case entryEncodingStandardWithACAndPriorityPrefixByte,
			entryEncodingSideloadedWithACAndPriorityPrefixByte:
			pri := getPriority(data[0])
			ramPri := raftAdmissionMeta.AdmissionPriority
			if int32(pri) != ramPri {
				panic(errors.AssertionFailedf("priorities are not equal: %d, %d", pri, ramPri))
			}
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
