// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protoutil

import "github.com/gogo/protobuf/proto"

// Message extends the proto.Message interface with the MarshalTo and Size
// methods we tell gogoproto to generate for us.
type Message interface {
	proto.Message
	MarshalTo(data []byte) (int, error)
	MarshalToSizedBuffer(data []byte) (int, error)
	Unmarshal(data []byte) error
	Size() int
}

// Interceptor will be called with every proto before it is marshaled.
// Interceptor is not safe to modify concurrently with calls to Marshal.
var Interceptor = func(_ Message) {}

// Marshal encodes pb into the wire format. It is used throughout the code base
// to intercept calls to proto.Marshal.
func Marshal(pb Message) ([]byte, error) {
	dest := make([]byte, pb.Size())
	if _, err := MarshalToSizedBuffer(pb, dest); err != nil {
		return nil, err
	}
	return dest, nil
}

// MarshalTo encodes pb into the wire format. It is used throughout the code
// base to intercept calls to pb.MarshalTo.
func MarshalTo(pb Message, dest []byte) (int, error) {
	Interceptor(pb)
	return pb.MarshalTo(dest)
}

// MarshalToSizedBuffer encodes pb into the wire format. It is used
// throughout the code base to intercept calls to pb.MarshalToSizedBuffer.
func MarshalToSizedBuffer(pb Message, dest []byte) (int, error) {
	Interceptor(pb)
	return pb.MarshalToSizedBuffer(dest)
}

// Unmarshal parses the protocol buffer representation in buf and places the
// decoded result in pb. If the struct underlying pb does not match the data in
// buf, the results can be unpredictable.
//
// Unmarshal resets pb before starting to unmarshal, so any existing data in pb
// is always removed.
func Unmarshal(data []byte, pb Message) error {
	pb.Reset()
	return pb.Unmarshal(data)
}
