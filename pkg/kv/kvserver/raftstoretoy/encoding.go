// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

type LogKeyKind byte

const (
	LKKIdent LogKeyKind = iota
	LGGSentinel
)

type LogEncodable interface {
	LogKeyKind() LogKeyKind
}

type Encoding interface {
	Encode(buf []byte, e LogEncodable) ([]byte, error)
	Decode(b []byte) (LogEncodable, error)
}
