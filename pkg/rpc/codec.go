// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/encoding"
)

const name = "proto"

type codec struct{}

var _ encoding.Codec = codec{}

func (codec) Marshal(v interface{}) ([]byte, error) {
	if pm, ok := v.(proto.Marshaler); ok {
		return pm.Marshal()
	}
	return protoutil.Marshal(v.(protoutil.Message))
}

func (codec) Unmarshal(data []byte, v interface{}) error {
	if pm, ok := v.(proto.Unmarshaler); ok {
		return pm.Unmarshal(data)
	}
	return protoutil.Unmarshal(data, v.(protoutil.Message))
}

func (codec) Name() string {
	return name
}
