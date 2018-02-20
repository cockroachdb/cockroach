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

package protoutil

import (
	"io"
	"io/ioutil"

	"github.com/gogo/protobuf/proto"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/pkg/errors"
)

var _ gwruntime.Marshaler = (*ProtoPb)(nil)

// ProtoPb is a gwruntime.Marshaler that uses github.com/gogo/protobuf/proto.
type ProtoPb struct{}

// ContentType implements gwruntime.Marshaler.
func (*ProtoPb) ContentType() string {
	// NB: This is the same as httputil.ProtoContentType which we can't use due
	// to an import cycle.
	const ProtoContentType = "application/x-protobuf"
	return ProtoContentType
}

// Marshal implements gwruntime.Marshaler.
func (*ProtoPb) Marshal(v interface{}) ([]byte, error) {
	// NB: we use proto.Message here because grpc-gateway passes us protos that
	// we don't control and thus don't implement protoutil.Message.
	if p, ok := v.(proto.Message); ok {
		return proto.Marshal(p)
	}
	return nil, errors.Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
}

// Unmarshal implements gwruntime.Marshaler.
func (*ProtoPb) Unmarshal(data []byte, v interface{}) error {
	// NB: we use proto.Message here because grpc-gateway passes us protos that
	// we don't control and thus don't implement protoutil.Message.
	if p, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data, p)
	}
	return errors.Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
}

// NewDecoder implements gwruntime.Marshaler.
func (*ProtoPb) NewDecoder(r io.Reader) gwruntime.Decoder {
	return gwruntime.DecoderFunc(func(v interface{}) error {
		// NB: we use proto.Message here because grpc-gateway passes us protos that
		// we don't control and thus don't implement protoutil.Message.
		if p, ok := v.(proto.Message); ok {
			bytes, err := ioutil.ReadAll(r)
			if err == nil {
				err = proto.Unmarshal(bytes, p)
			}
			return err
		}
		return errors.Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
	})
}

// NewEncoder implements gwruntime.Marshaler.
func (*ProtoPb) NewEncoder(w io.Writer) gwruntime.Encoder {
	return gwruntime.EncoderFunc(func(v interface{}) error {
		// NB: we use proto.Message here because grpc-gateway passes us protos that
		// we don't control and thus don't implement protoutil.Message.
		if p, ok := v.(proto.Message); ok {
			bytes, err := proto.Marshal(p)
			if err == nil {
				_, err = w.Write(bytes)
			}
			return err
		}
		return errors.Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
	})
}

var _ gwruntime.Delimited = (*ProtoPb)(nil)

// Delimiter implements gwruntime.Delimited.
func (*ProtoPb) Delimiter() []byte {
	return nil
}
