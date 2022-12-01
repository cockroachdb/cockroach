// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package testingint

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	gogoproto "github.com/gogo/protobuf/proto"
)

// RealTestingInt64 is an int64 with methods that allow it to be used as a
// `gogoproto.casttype`, and which has a getter/setter. See
// `buildutil.TestingInt64`.
type RealTestingInt64 int64

var _ protoutil.Message = (*RealTestingInt64)(nil)

func (m *RealTestingInt64) Reset() {
	*m = 0
}

// String implements (a part of) protoutil.Message.
func (m *RealTestingInt64) String() string {
	return fmt.Sprint(*m)
}

// ProtoMessage implements (a part of) protoutil.Message.
func (m *RealTestingInt64) ProtoMessage() {
}

// MarshalTo implements (a part of) protoutil.Message.
func (m *RealTestingInt64) MarshalTo(buf []byte) (int, error) {
	sl := gogoproto.EncodeVarint(uint64(*m))
	_ = append(buf[:0], sl...)
	return len(sl), nil
}

// Unmarshal implements (a part of) protoutil.Message.
func (m *RealTestingInt64) Unmarshal(buf []byte) error {
	x, n := gogoproto.DecodeVarint(buf)
	if n == 0 {
		return errors.Errorf("unable to unmarshal %x as varint", buf)
	}
	*m = RealTestingInt64(x)
	return nil
}

// MarshalToSizedBuffer implements (a part of) protoutil.Message.
func (m *RealTestingInt64) MarshalToSizedBuffer(buf []byte) (int, error) {
	sl := gogoproto.EncodeVarint(uint64(*m))
	_ = append(buf[:len(buf)-len(sl)], sl...)
	return len(sl), nil
}

// Size implements (a part of) protoutil.Message.
func (m *RealTestingInt64) Size() int {
	return len(gogoproto.EncodeVarint(uint64(*m)))
}

// Equal implements (gogoproto.equal).
func (m *RealTestingInt64) Equal(n interface{}) bool {
	return reflect.DeepEqual(m, n)
}

// Set updates the receiver. Not thread safe.
func (m *RealTestingInt64) Set(n int64) {
	*m = RealTestingInt64(n)
}

// Get reads the receiver. Not thread safe.
func (m RealTestingInt64) Get() int64 {
	return int64(m)
}
