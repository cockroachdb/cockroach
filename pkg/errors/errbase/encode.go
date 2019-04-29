// Copyright 2019 The Cockroach Authors.
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

package errbase

import (
	"context"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/errors/errorspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gogo/protobuf/types"
)

// EncodedError is the type of an encoded (and protobuf-encodable) error.
type EncodedError = errorspb.EncodedError

// EncodeError encodes an error.
func EncodeError(err error) EncodedError {
	if cause := UnwrapOnce(err); cause != nil {
		return encodeWrapper(err, cause)
	}
	// Not a causer.
	return encodeLeaf(err)
}

// encodeLeaf encodes a leaf error.
func encodeLeaf(err error) EncodedError {
	var msg string
	var safe []string
	var typeName string
	var fullDetails *types.Any

	if e, ok := err.(*opaqueLeaf); ok {
		msg = e.msg
		safe = e.safeDetails
		typeName = e.typeName
		fullDetails = e.payload
	} else {
		typeName = FullTypeName(err)
		var payload protoutil.SimpleMessage

		// If we have a manually registered encoder, use that.
		if enc, ok := leafEncoders[typeName]; ok {
			msg, safe, payload = enc(err)
		} else {
			// No encoder. Let's try to manually extract fields.

			// The message comes from Error(). Simple.
			msg = err.Error()

			// If there are known safe details, use them.
			if s, ok := err.(SafeDetailer); ok {
				safe = s.SafeDetails()
			}

			// If it's also a protobuf message, we'll use that as
			// payload. DecodeLeaf() will know how to turn that back into a
			// full error if there is no decoder.
			payload, _ = err.(protoutil.SimpleMessage)
		}
		// If there is a detail payload, encode it.
		fullDetails = encodeAsAny(err, payload)
	}

	return EncodedError{
		Error: &errorspb.EncodedError_Leaf{
			Leaf: &errorspb.EncodedErrorLeaf{
				Message:           msg,
				TypeName:          typeName,
				ReportablePayload: safe,
				FullDetails:       fullDetails,
			},
		},
	}
}

func encodeAsAny(err error, payload protoutil.SimpleMessage) *types.Any {
	if payload == nil {
		return nil
	}

	any, marshalErr := types.MarshalAny(payload)
	if marshalErr != nil {
		log.Warningf(context.Background(),
			"error %+v (%T) announces proto message, but marshaling fails: %+v",
			err, err, marshalErr)
		return nil
	}

	return any
}

// encodeWrapper encodes an error wrapper.
func encodeWrapper(err, cause error) EncodedError {
	var msg string
	var safe []string
	var typeName string
	var fullDetails *types.Any

	if e, ok := err.(*opaqueWrapper); ok {
		msg = e.prefix
		safe = e.safeDetails
		typeName = e.typeName
		fullDetails = e.payload
	} else {
		typeName = FullTypeName(err)
		var payload protoutil.SimpleMessage

		// If we have a manually registered encoder, use that.
		if enc, ok := encoders[typeName]; ok {
			msg, safe, payload = enc(err)
		} else {
			// No encoder.
			// In that case, we'll try to compute a message prefix
			// manually.
			msg = extractPrefix(err, cause)

			// If there are known safe details, use them.
			if s, ok := err.(SafeDetailer); ok {
				safe = s.SafeDetails()
			}

			// That's all we can get.
		}
		// If there is a detail payload, encode it.
		fullDetails = encodeAsAny(err, payload)
	}

	return EncodedError{
		Error: &errorspb.EncodedError_Wrapper{
			Wrapper: &errorspb.EncodedWrapper{
				Cause:             EncodeError(cause),
				MessagePrefix:     msg,
				TypeName:          typeName,
				ReportablePayload: safe,
				FullDetails:       fullDetails,
			},
		},
	}
}

// extractPrefix extracts the prefix from a wrapper's error message.
// For example,
//    err := errors.New("bar")
//    err = errors.Wrap(err, "foo")
//    extractPrefix(err)
// returns "foo".
func extractPrefix(err, cause error) string {
	causeSuffix := cause.Error()
	errMsg := err.Error()

	if strings.HasSuffix(errMsg, causeSuffix) {
		prefix := errMsg[:len(errMsg)-len(causeSuffix)]
		if strings.HasSuffix(prefix, ": ") {
			return prefix[:len(prefix)-2]
		}
	}
	return ""
}

// FullTypeName returns a qualified Go type name for the given object.
func FullTypeName(err interface{}) string {
	// If we have received an error of type not known locally,
	// we still know its type name. Return that.
	switch t := err.(type) {
	case *opaqueLeaf:
		return t.typeName
	case *opaqueWrapper:
		return t.typeName
	}

	t := reflect.TypeOf(err)
	typeName := t.String()
	pkgPath := getPkgPath(t)
	if pkgPath != "" {
		return pkgPath + "/" + typeName
	}
	return typeName
}

type TypeNamer interface {
	FullErrorTypeName() string
}

// getPkgPath extract the package path for a Go type. We'll do some
// extra work for typical types that did not get a name, for example
// *E has the package path of E.
func getPkgPath(t reflect.Type) string {
	pkgPath := t.PkgPath()
	if pkgPath != "" {
		return pkgPath
	}
	// Try harder.
	switch t.Kind() {
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Ptr, reflect.Slice:
		return getPkgPath(t.Elem())
	}
	// Nothing to report.
	return ""
}

// RegisterLeafEncoder can be used to register new leaf error types to
// the library. Registered types will be encoded using their own
// Go type when an error is encoded. Wrappers that have not been
// registered will be encoded using the opaqueLeaf type.
func RegisterLeafEncoder(typeName string, encoder LeafEncoder) {
	if encoder == nil {
		delete(leafEncoders, typeName)
	} else {
		leafEncoders[typeName] = encoder
	}
}

// LeafEncoder is to be provided (via RegisterLeafEncoder above)
// by additional wrapper types not yet known to this library.
type LeafEncoder func(err error) (msg string, safeDetails []string, payload protoutil.SimpleMessage)

// registry for RegisterLeafEncoder.
var leafEncoders = map[string]LeafEncoder{}

// RegisterWrapperEncoder can be used to register new wrapper types to
// the library. Registered wrappers will be encoded using their own
// Go type when an error is encoded. Wrappers that have not been
// registered will be encoded using the opaqueWrapper type.
func RegisterWrapperEncoder(typeName string, encoder WrapperEncoder) {
	if encoder == nil {
		delete(encoders, typeName)
	} else {
		encoders[typeName] = encoder
	}
}

// WrapperEncoder is to be provided (via RegisterWrapperEncoder above)
// by additional wrapper types not yet known to this library.
type WrapperEncoder func(err error) (msgPrefix string, safeDetails []string, payload protoutil.SimpleMessage)

// registry for RegisterWrapperType.
var encoders = map[string]WrapperEncoder{}
