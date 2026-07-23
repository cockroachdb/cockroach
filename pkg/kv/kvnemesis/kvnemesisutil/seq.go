// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesisutil

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	gogoproto "github.com/gogo/protobuf/proto"
)

// Seq is a unique identifier used to associate MVCC versions with the kvnemesis
// operation that wrote them.
type Seq uint32

func (s Seq) String() string {
	return fmt.Sprintf("s%d", s)
}

// SeqContainer is an uint32 with methods that allow it to be used as a
// `gogoproto.casttype`, and which has a getter/setter. See
type SeqContainer uint32

var _ protoutil.Message = (*SeqContainer)(nil)

// IsEmpty is used by generated marshalling code (with gogoproto.omitempty
// extension).
func (m SeqContainer) IsEmpty() bool {
	return m == 0
}

// Reset implements (a part of) protoutil.Message.
func (m *SeqContainer) Reset() {
	*m = 0
}

// ProtoMessage implements (a part of) protoutil.Message.
func (m SeqContainer) String() string {
	return fmt.Sprintf("s%d", m)
}

// ProtoMessage implements (a part of) protoutil.Message.
func (m *SeqContainer) ProtoMessage() {
}

// MarshalTo implements (a part of) protoutil.Message.
func (m *SeqContainer) MarshalTo(buf []byte) (int, error) {
	if *m == 0 {
		return 0, nil
	}
	sl := gogoproto.EncodeVarint(uint64(*m))
	_ = append(buf[:0], sl...)
	return len(sl), nil
}

type errT string

func (err errT) Error() string {
	return string(err)
}

// Unmarshal implements (a part of) protoutil.Message.
func (m *SeqContainer) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		*m = 0
		return nil
	}
	x, n := gogoproto.DecodeVarint(buf)
	if n == 0 {
		return errT(fmt.Sprintf("unable to unmarshal %x as varint", buf))
	}
	*m = SeqContainer(x)
	return nil
}

// MarshalToSizedBuffer implements (a part of) protoutil.Message.
func (m *SeqContainer) MarshalToSizedBuffer(buf []byte) (int, error) {
	if *m == 0 {
		return 0, nil
	}
	sl := gogoproto.EncodeVarint(uint64(*m))
	_ = append(buf[:len(buf)-len(sl)], sl...)
	return len(sl), nil
}

// Size implements (a part of) protoutil.Message.
func (m *SeqContainer) Size() int {
	if *m == 0 {
		return 0
	}
	return len(gogoproto.EncodeVarint(uint64(*m)))
}

// Equal implements (gogoproto.equal).
func (m *SeqContainer) Equal(n interface{}) bool {
	return reflect.DeepEqual(m, n)
}

// Set updates the receiver. Not thread safe.
func (m *SeqContainer) Set(seq Seq) {
	*m = SeqContainer(seq)
}

// Get reads the receiver. Not thread safe.
func (m SeqContainer) Get() Seq {
	return Seq(m)
}

// NoopContainer is an empty struct that can be used as a `gogoproto.casttype` in
// proto messages. It uses no space. When the crdb_test build tag is set, this
// type is instead represented by a NoopContainer.
type NoopContainer struct{}

func (m *NoopContainer) IsEmpty() bool { return true }

func (m *NoopContainer) Reset() {}

// String implements (a part of) protoutil.Message.
func (m *NoopContainer) String() string { return "0" }

// ProtoMessage implements (a part of) protoutil.Message.
func (m *NoopContainer) ProtoMessage() {}

// MarshalTo implements (a part of) protoutil.Message.
func (m *NoopContainer) MarshalTo(buf []byte) (int, error) { return 0, nil }

// Unmarshal implements (a part of) protoutil.Message.
func (m *NoopContainer) Unmarshal(buf []byte) error { return nil }

// MarshalToSizedBuffer implements (a part of) protoutil.Message.
func (m *NoopContainer) MarshalToSizedBuffer(buf []byte) (int, error) { return 0, nil }

// Size implements (a part of) protoutil.Message.
func (m *NoopContainer) Size() int { return 0 }

// Equal implements (gogoproto.equal).
func (m *NoopContainer) Equal(n interface{}) bool { return reflect.DeepEqual(m, n) }

// Set is a no-op.
func (m *NoopContainer) Set(Seq) {}

// Get returns zero.
func (m NoopContainer) Get() Seq { return 0 }
