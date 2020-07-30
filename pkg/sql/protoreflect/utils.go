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
	"encoding/json"
	"reflect"

	jsonb "github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

// MessageToJSON converts a protocol message, represented by its fully qualified
// name, and it's marshaled data, into a JSONB object.
func MessageToJSON(name string, data []byte) (jsonb.JSON, error) {
	// Get the reflected type of the protocol message.
	rt := proto.MessageType(name)
	if rt == nil {
		return nil, errors.Newf("unknown proto message type %s", name)
	}

	// If the message is known, we should get the pointer to our message.
	if rt.Kind() != reflect.Ptr {
		return nil, errors.Newf("expected ptr to message, got %s instead", rt.Kind().String())
	}

	// Construct message of appropriate type, through reflection.
	rt = rt.Elem()
	rv := reflect.New(rt)

	// NB: we use proto.Message here because we want to be able to decode any
	// protocol message (e.g. from grpc gateway, etc).
	msg, ok := rv.Interface().(proto.Message)

	if !ok {
		// Just to be safe;
		return nil, errors.AssertionFailedf(
			"unexpected proto type for %s; expected proto.Message, got %T",
			name, rv.Interface())
	}

	// Now, parse data as our proto message.
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal proto %s", name)
	}

	// Convert to json.
	jsonEncoder := jsonpb.Marshaler{}
	msgJSON, err := jsonEncoder.MarshalToString(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "marshaling %s; msg=%+v", name, msg)
	}

	// We need to take encoded json object and, unfortunately, unmarshal
	// it again, this time as a string->value map, so that we can construct
	// JSONB object to return.
	var msgMap map[string]interface{}
	if err := json.Unmarshal([]byte(msgJSON), &msgMap); err != nil {
		return nil, errors.Wrap(err, "unmarshaling to map")
	}

	// Build JSONB.
	builder := jsonb.NewObjectBuilder(len(msgMap))
	for k, v := range msgMap {
		jv, err := jsonb.MakeJSON(v)
		if err != nil {
			return nil, errors.Wrapf(err, "encoding json value for %s", k)
		}
		builder.Add(k, jv)
	}
	return builder.Build(), nil
}
