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
	json_encoding "encoding/json"
	"reflect"
	"strconv"
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

// makeJSON exists so that the encoding produced by MessageToJSON is round
// trip-able: that is, we can take that encoding and convert it back to the
// protocol message representation.
// See encoding comment in MessageToJSON below.
func makeJSON(i interface{}) (jsonb.JSON, error) {
	switch val := i.(type) {
	case float64:
		return jsonb.FromString(strconv.FormatFloat(val, 'f', -1, 64)), nil
	case map[string]interface{}:
		b := jsonb.NewObjectBuilder(len(val))
		for k, v := range val {
			j, err := makeJSON(v)
			if err != nil {
				return nil, err
			}
			b.Add(k, j)
		}
		return b.Build(), nil
	case []interface{}:
		b := jsonb.NewArrayBuilder(len(val))
		for _, v := range val {
			j, err := makeJSON(v)
			if err != nil {
				return nil, err
			}
			b.Add(j)
		}
		return b.Build(), nil
	default:
		return jsonb.MakeJSON(val)
	}
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

// MessageToJSON converts a protocol message into a JSONB object.
func MessageToJSON(msg protoutil.Message) (jsonb.JSON, error) {
	// Convert to json.
	jsonEncoder := jsonpb.Marshaler{EmitDefaults: false}
	msgJSON, err := jsonEncoder.MarshalToString(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "marshaling %s; msg=%+v", proto.MessageName(msg), msg)
	}

	// We need to take encoded json object and, unfortunately, unmarshal
	// it again, this time as a string->value map, so that we can construct
	// JSONB object to return.
	//
	// NB: this unfortunate conversation to map[string]interface{} also
	// creates a problem if we want to take JSONB returned by this function,
	// and convert it back to protocol message.
	// json_encoding.Unmarshal encodes *all* numeric values as float64.
	// This has the unfortunate effect that when we construct our JSONB object
	// below, those numbers will be encoded using scientific notation
	// due to how APD library formats float64 (e.g. instead of "50" for table id,
	// we will emit "5E1").  This, in turn causes problems if we attempt
	// to unmarshal this json as a protocol message.
	// TODO(yevgeniy): It would be nice to be able to pass formatting context to APD.
	// TODO(yevgeniy): It would be even better if we didn't have to convert
	//    from msgJSON to map[] and instead, be able to build jsonb object directly.
	var msgMap map[string]interface{}
	if err := json_encoding.Unmarshal([]byte(msgJSON), &msgMap); err != nil {
		return nil, errors.Wrap(err, "unmarshaling to map")
	}

	// Build JSONB.
	builder := jsonb.NewObjectBuilder(len(msgMap))
	for k, v := range msgMap {
		jv, err := makeJSON(v)
		if err != nil {
			return nil, errors.Wrapf(err, "encoding json value for %s", k)
		}
		builder.Add(k, jv)
	}
	return builder.Build(), nil
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
