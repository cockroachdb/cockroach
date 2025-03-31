// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rspb

//go:generate stringer --type=KeyKind --linecomment

type KeyKind byte

const (
	KeyKindUnknown KeyKind = iota
	KeyKindLogIDGenerator
	KeyKindRaftLogHardState
	KeyKindRaftLogTruncatedState
	KeyKindRaftLogInit
	KeyKindRaftLogDestroy
	KeyKindRaftLogApplyingIndex
	KeyKindRaftLogEntry
	KeyKindSentinel
)

var KeyKindByString = make(map[string]KeyKind)

func init() {
	for i := KeyKindUnknown; i < KeyKindSentinel; i++ {
		KeyKindByString[i.String()] = i
	}
}
