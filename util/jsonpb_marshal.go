// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package util

import (
	"bytes"
	"io"

	gwruntime "github.com/gengo/grpc-gateway/runtime"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

var _ gwruntime.Marshaler = (*JSONPb)(nil)

// JSONPb is a gwruntime.Marshaler that uses github.com/gogo/protobuf/jsonpb.
type JSONPb jsonpb.Marshaler

// ContentType implements gwruntime.Marshaler.
func (*JSONPb) ContentType() string {
	return "application/json"
}

// Marshal implements gwruntime.Marshaler.
func (j *JSONPb) Marshal(v interface{}) ([]byte, error) {
	if pb, ok := v.(proto.Message); ok {
		var buf bytes.Buffer
		marshalFn := (*jsonpb.Marshaler)(j).Marshal
		if err := marshalFn(&buf, pb); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return nil, Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
}

// Unmarshal implements gwruntime.Marshaler.
func (j *JSONPb) Unmarshal(data []byte, v interface{}) error {
	if pb, ok := v.(proto.Message); ok {
		return jsonpb.Unmarshal(bytes.NewReader(data), pb)
	}
	return Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
}

// NewDecoder implements gwruntime.Marshaler.
func (j *JSONPb) NewDecoder(r io.Reader) gwruntime.Decoder {
	return gwruntime.DecoderFunc(func(v interface{}) error {
		if pb, ok := v.(proto.Message); ok {
			return jsonpb.Unmarshal(r, pb)
		}
		return Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
	})
}

// NewEncoder implements gwruntime.Marshaler.
func (j *JSONPb) NewEncoder(w io.Writer) gwruntime.Encoder {
	return gwruntime.EncoderFunc(func(v interface{}) error {
		if pb, ok := v.(proto.Message); ok {
			marshalFn := (*jsonpb.Marshaler)(j).Marshal
			return marshalFn(w, pb)
		}
		return Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
	})
}
