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

var shorthands map[string]protoutil.Message = map[string]protoutil.Message{}

// RegisterShorthands registers a shorthand alias for a given message type which
// can be used by NewMessage to look up that message type, when it fails to find
// a message type with that name in the fully-qualified global registry first.
// Aliases are folded to lower-case with any '_'s removed prior to registration
// and when being searched.
func RegisterShorthands(msg protoutil.Message, names ...string) {
	for _, name := range names {
		name = foldShorthand(name)
		if existing, ok := shorthands[name]; ok {
			panic(errors.AssertionFailedf("shorthand %s already registered to %T", name, existing))
		}
		shorthands[name] = msg
	}
}

func foldShorthand(name string) string {
	return strings.ReplaceAll(strings.ToLower(name), "_", "")
}

// NewMessage creates a new protocol message object, given its fully
// qualified name, or an alias previously registered via RegisterShorthands.
func NewMessage(name string) (protoutil.Message, error) {
	// Get the reflected type of the protocol message.
	rt := proto.MessageType(name)
	if rt == nil {
		if msg, ok := shorthands[foldShorthand(name)]; ok {
			fullName := proto.MessageName(msg)
			rt = proto.MessageType(fullName)
			if rt == nil {
				return nil, errors.Newf("unknown proto message type %s", fullName)
			}
		} else {
			return nil, errors.Newf("unknown proto message type %s", name)
		}
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

// FmtFlags is a struct describing how MessageToJSON ought to format its output.
type FmtFlags struct {
	// EmitDefaults flag dictates whether fields with zero values are rendered or not.
	EmitDefaults bool
	// EmitRedacted indicates redacted JSON representation is needed.
	EmitRedacted bool
}

func marshalToJSON(msg protoutil.Message, emitDefaults bool) (jsonb.JSON, error) {
	jsonEncoder := jsonpb.Marshaler{EmitDefaults: emitDefaults}
	msgJSON, err := jsonEncoder.MarshalToString(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "marshaling %s; msg=%+v", proto.MessageName(msg), msg)
	}
	return jsonb.ParseJSON(msgJSON)
}

// MessageToJSON converts a protocol message into a JSONB object.
func MessageToJSON(msg protoutil.Message, flags FmtFlags) (jsonb.JSON, error) {
	if flags.EmitRedacted {
		return marshalToJSONRedacted(msg, flags.EmitDefaults)
	}
	return marshalToJSON(msg, flags.EmitDefaults)
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
