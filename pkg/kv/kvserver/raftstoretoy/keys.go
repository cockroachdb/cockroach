// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftstoretoy/logpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type LogMetaKeyType byte

const (
	LMKHardState LogMetaKeyType = iota
	LMKInit
	LMKDestroy
	// NB: log entries have their own struct due to storing an extra int
)

type LogMetaKey struct {
	RangeID roachpb.RangeID
	LogID   logpb.LogID
	Typ     LogMetaKeyType
}

type LogRaftEntryKey struct {
	LogMetaKey
	Index RaftIndex
}

/*

import (
	"bytes"
	"fmt"
)

var sep = []byte{'/'}

// Key is a component-decomposed key.
// Honor system: don't include `sep` in any component.
type Key [][]byte

func (k Key) String() string {
	return string(k.Encode())
}

func (k Key) Encode() []byte {
	return bytes.Join(k, sep)
}

func DecodeKey(b []byte) Key {
	return bytes.Split(b, sep)
}

func MakeKey(components ...any) Key {
	var sl Key
	for _, s := range components {
		sl = append(sl, []byte(fmt.Sprint(s)))
	}
	return sl

}
*/
