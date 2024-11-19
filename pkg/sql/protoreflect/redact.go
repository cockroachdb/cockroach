// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package protoreflect

import (
	"fmt"
	"reflect"
	"strings"

	jsonb "github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

// anyResolver implements jsonpb.AnyResolver
// This implementation exists as a hack to enable passing the flag "EmitRedacted"
// to the jsonbpb.JSONPBMarshaler implementations.
// TODO(yevgeniy): Eliminate this hack; this likely involves forking jsonbpb package.
type anyResolver struct{}

// Resolve implements jsonpb.AnyResolver interface.
// This method is a copy of defaultResolveAny from jsonpb package.
func (r *anyResolver) Resolve(typeURL string) (proto.Message, error) {
	// Only the part of typeURL after the last slash is relevant.
	mname := typeURL
	if slash := strings.LastIndex(mname, "/"); slash >= 0 {
		mname = mname[slash+1:]
	}
	mt := proto.MessageType(mname)
	if mt == nil {
		return nil, fmt.Errorf("unknown message type %q", mname)
	}
	return reflect.New(mt.Elem()).Interface().(proto.Message), nil
}

// ShouldRedact returns true if the implementers of jsonpb.JSONPBMarshaller interface
// should redact the returned data.
func ShouldRedact(m *jsonpb.Marshaler) bool {
	_, shouldRedact := m.AnyResolver.(*anyResolver)
	return shouldRedact
}

// redactionJSONBMarker is a JSON map that is concatenated with
// the JSONB object produced by MessageToJSON to mark the message as redacted.
// This marker is added so that resulting JSONB cannot be used directly as an argument
// to the crdb_internal.json_to_pb call (a safety mechanism to prevent accidental
// overrides with redacted data).
var redactionJSONBMarker = func() jsonb.JSON {
	jb, err := jsonb.ParseJSON(`{"__redacted__": true}`)
	if err != nil {
		panic("unexpected error parsing redaction JSON")
	}
	return jb
}()

func marshalToJSONRedacted(msg protoutil.Message, emitDefaults bool) (jsonb.JSON, error) {
	jsonEncoder := jsonpb.Marshaler{EmitDefaults: emitDefaults}
	jsonEncoder.AnyResolver = &anyResolver{}
	msgJSON, err := jsonEncoder.MarshalToString(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "marshaling %s; msg=%+v", proto.MessageName(msg), msg)
	}
	jb, err := jsonb.ParseJSON(msgJSON)
	if err != nil {
		return nil, err
	}
	return jb.Concat(redactionJSONBMarker)
}
