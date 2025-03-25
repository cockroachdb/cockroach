// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

type LogKeyKind byte

const (
	LKKIdent LogKeyKind = iota
	LKKInvalid
)

type LogEncodable interface {
	LogKeyKind() LogKeyKind
}

type Encoding interface {
	Register(kind LogKeyKind, new func() LogEncodable)

	Encode(buf []byte, e LogEncodable) ([]byte, error)
	Decode(b []byte, dstOrNil LogEncodable) (LogEncodable, error)
}

func RegisterAll(enc Encoding) {
	enc.Register(LKKIdent, func() LogEncodable {
		return &FullLogID{}
	})
}
