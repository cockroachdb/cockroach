// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protoreflect

import (
	"reflect"
	"strings"

	jsonb "github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

// NewMessage creates a new protocol message object, given its fully
// qualified name.
func NewMessage(name string) (protoutil.Message, error) {
	// Get the reflected type of the protocol message.
	rt := proto.MessageType(name)
	if rt == nil {
		return nil, errors.Newf("unknown proto message type %s", name)
	}

	// If the message is known, we should get the pointer to our message.
	if rt.Kind() != reflect.Ptr {
		return nil, errors.AssertionFailedf(
			"expected ptr to message, got %s instead", rt.Kind().String())
	}

	// Construct message of appropriate type, through reflection.
	rv := reflect.New(rt.Elem())
	msg, ok := rv.Interface().(protoutil.Message)

	if !ok {
		// Just to be safe;
		return nil, errors.AssertionFailedf(
			"unexpected proto type for %s; expected protoutil.Message, got %T",
			name, rv.Interface())
	}
	return msg, nil
}

// DecodeMessage decodes protocol message specified as its fully
// qualified name, and it's marshaled data, into a protoutil.Message.
func DecodeMessage(name string, data []byte) (protoutil.Message, error) {
	msg, err := NewMessage(name)
	if err != nil {
		return nil, err
	}

	// Now, parse data as our proto message.
	if err := protoutil.Unmarshal(data, msg); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal proto %s", name)
	}
	return msg, nil
}

// MessageToJSON converts a protocol message into a JSONB object. The
// emitDefaults flag dictates whether fields with zero values are rendered or
// not.
func MessageToJSON(msg protoutil.Message, emitDefaults bool) (jsonb.JSON, error) {
	// Convert to json.
	jsonEncoder := jsonpb.Marshaler{EmitDefaults: emitDefaults}
	msgJSON, err := jsonEncoder.MarshalToString(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "marshaling %s; msg=%+v", proto.MessageName(msg), msg)
	}

	return jsonb.ParseJSON(msgJSON)
}

// JSONBMarshalToMessage initializes the target protocol message with
// the data in the input JSONB object.
// Returns serialized byte representation of the protocol message.
func JSONBMarshalToMessage(input jsonb.JSON, target protoutil.Message) ([]byte, error) {
	json := &jsonpb.Unmarshaler{}
	if err := json.Unmarshal(strings.NewReader(input.String()), target); err != nil {
		return nil, errors.Wrapf(err, "unmarshaling json to %s", proto.MessageName(target))
	}
	data, err := protoutil.Marshal(target)
	if err != nil {
		return nil, errors.Wrapf(err, "marshaling to proto %s", proto.MessageName(target))
	}
	return data, nil
}
