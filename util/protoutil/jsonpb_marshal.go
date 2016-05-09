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

package protoutil

import (
	"bytes"
	"fmt"
	"io"
	"reflect"

	gwruntime "github.com/gengo/grpc-gateway/runtime"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

var typeProtoMessage = reflect.TypeOf((*proto.Message)(nil)).Elem()

// JSONPb is a Marshaler which marshals/unmarshals into/from JSON
// with the "github.com/gogo/protobuf/jsonpb".
// It supports fully functionality of protobuf unlike JSONBuiltin.
type JSONPb jsonpb.Marshaler

// ContentType always returns "application/json".
func (*JSONPb) ContentType() string {
	return "application/json"
}

// Marshal marshals "v" into JSON
func (j *JSONPb) Marshal(v interface{}) ([]byte, error) {
	if pb, ok := v.(proto.Message); ok {
		var buf bytes.Buffer
		marshalFn := (*jsonpb.Marshaler)(j).Marshal
		if err := marshalFn(&buf, pb); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return nil, fmt.Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
}

// Unmarshal unmarshals JSON "data" into "v"
func (j *JSONPb) Unmarshal(data []byte, v interface{}) error {
	if pb, ok := v.(proto.Message); ok {
		return jsonpb.Unmarshal(bytes.NewReader(data), pb)
	}
	return fmt.Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
}

// NewDecoder returns a Decoder which reads JSON stream from "r".
func (j *JSONPb) NewDecoder(r io.Reader) gwruntime.Decoder {
	return gwruntime.DecoderFunc(func(v interface{}) error {
		if pb, ok := v.(proto.Message); ok {
			return jsonpb.Unmarshal(r, pb)
		}
		return fmt.Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
	})
}

// NewEncoder returns an Encoder which writes JSON stream into "w".
func (j *JSONPb) NewEncoder(w io.Writer) gwruntime.Encoder {
	return gwruntime.EncoderFunc(func(v interface{}) error {
		if pb, ok := v.(proto.Message); ok {
			marshalFn := (*jsonpb.Marshaler)(j).Marshal
			return marshalFn(w, pb)
		}
		return fmt.Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
	})
}
