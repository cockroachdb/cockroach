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

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
)

var _ gwruntime.Marshaler = (*JSONPb)(nil)

var typeProtoMessage = reflect.TypeOf((*proto.Message)(nil)).Elem()

// JSONPb is a gwruntime.Marshaler that uses github.com/gogo/protobuf/jsonpb.
type JSONPb jsonpb.Marshaler

// ContentType implements gwruntime.Marshaler.
func (*JSONPb) ContentType() string {
	// NB: This is the same as httputil.JSONContentType which we can't use due to
	// an import cycle.
	const JSONContentType = "application/json"
	return JSONContentType
}

// Marshal implements gwruntime.Marshaler.
func (j *JSONPb) Marshal(v interface{}) ([]byte, error) {
	return j.marshal(v)
}

// a lower-case version of marshal to allow for a call from
// marshalNonProtoField without upsetting TestProtoMarshal().
func (j *JSONPb) marshal(v interface{}) ([]byte, error) {
	// NB: we use proto.Message here because grpc-gateway passes us protos that
	// we don't control and thus don't implement protoutil.Message.
	if pb, ok := v.(proto.Message); ok {
		var buf bytes.Buffer
		marshalFn := (*jsonpb.Marshaler)(j).Marshal
		if err := marshalFn(&buf, pb); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return j.marshalNonProtoField(v)
}

// Cribbed verbatim from grpc-gateway.
type protoEnum interface {
	fmt.Stringer
	EnumDescriptor() ([]byte, []int)
}

// Cribbed verbatim from grpc-gateway.
func (j *JSONPb) marshalNonProtoField(v interface{}) ([]byte, error) {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return []byte("null"), nil
		}
		rv = rv.Elem()
	}

	if rv.Kind() == reflect.Map {
		m := make(map[string]*json.RawMessage)
		for _, k := range rv.MapKeys() {
			buf, err := j.marshal(rv.MapIndex(k).Interface())
			if err != nil {
				return nil, err
			}
			m[fmt.Sprintf("%v", k.Interface())] = (*json.RawMessage)(&buf)
		}
		if j.Indent != "" {
			return json.MarshalIndent(m, "", j.Indent)
		}
		return json.Marshal(m)
	}
	if enum, ok := rv.Interface().(protoEnum); ok && !j.EnumsAsInts {
		return json.Marshal(enum.String())
	}
	return json.Marshal(rv.Interface())
}

// Unmarshal implements gwruntime.Marshaler.
func (j *JSONPb) Unmarshal(data []byte, v interface{}) error {
	// NB: we use proto.Message here because grpc-gateway passes us protos that
	// we don't control and thus don't implement protoutil.Message.
	if pb, ok := v.(proto.Message); ok {
		return jsonpb.Unmarshal(bytes.NewReader(data), pb)
	}
	return errors.Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
}

// NewDecoder implements gwruntime.Marshaler.
func (j *JSONPb) NewDecoder(r io.Reader) gwruntime.Decoder {
	return gwruntime.DecoderFunc(func(v interface{}) error {
		// NB: we use proto.Message here because grpc-gateway passes us protos that
		// we don't control and thus don't implement protoutil.Message.
		if pb, ok := v.(proto.Message); ok {
			return jsonpb.Unmarshal(r, pb)
		}
		return errors.Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
	})
}

// NewEncoder implements gwruntime.Marshaler.
func (j *JSONPb) NewEncoder(w io.Writer) gwruntime.Encoder {
	return gwruntime.EncoderFunc(func(v interface{}) error {
		// NB: we use proto.Message here because grpc-gateway passes us protos that
		// we don't control and thus don't implement protoutil.Message.
		if pb, ok := v.(proto.Message); ok {
			marshalFn := (*jsonpb.Marshaler)(j).Marshal
			return marshalFn(w, pb)
		}
		return errors.Errorf("unexpected type %T does not implement %s", v, typeProtoMessage)
	})
}

var _ gwruntime.Delimited = (*JSONPb)(nil)

// Delimiter implements gwruntime.Delimited.
func (*JSONPb) Delimiter() []byte {
	return []byte("\n")
}
