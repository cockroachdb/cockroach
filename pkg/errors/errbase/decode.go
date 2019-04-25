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

	"github.com/cockroachdb/cockroach/pkg/errors/errorspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gogo/protobuf/types"
)

// DecodeError decodes an error.
func DecodeError(enc EncodedError) error {
	if w := enc.GetWrapper(); w != nil {
		return decodeWrapper(w)
	}
	return decodeLeaf(enc.GetLeaf())
}

func decodeLeaf(enc *errorspb.EncodedErrorLeaf) error {
	// In case there is a detailed payload, decode it.
	var payload protoutil.SimpleMessage
	if enc.FullDetails != nil {
		var d types.DynamicAny
		err := types.UnmarshalAny(enc.FullDetails, &d)
		if err != nil {
			// It's OK if we can't decode. We'll use
			// the opaque type below.
			log.Warningf(context.Background(), "error while unmarshalling error: %+v", err)
		} else {
			payload = d.Message
		}
	}

	// Do we have a leaf decoder for this type?
	if decoder, ok := leafDecoders[enc.TypeName]; ok {
		// Yes, use it.
		genErr := decoder(enc.Message, enc.ReportablePayload, payload)
		if genErr != nil {
			// Decoding succeeded. Use this.
			return genErr
		}
		// Decoding failed, we'll drop through to opaqueLeaf{} below.
	} else {
		// Shortcut for non-registered proto-encodable error types:
		// if it already implements `error`, it's good to go.
		if e, ok := payload.(error); ok {
			// yes: we're done!
			return e
		}
	}

	// No decoder and no error type: we'll keep what we received and
	// make it ready to re-encode exactly (if the error leaves over the
	// network again).
	return &opaqueLeaf{
		msg:         enc.Message,
		typeName:    enc.TypeName,
		safeDetails: enc.ReportablePayload,
		payload:     enc.FullDetails,
	}
}

func decodeWrapper(enc *errorspb.EncodedWrapper) error {
	// First decode the cause.
	cause := DecodeError(enc.Cause)

	// In case there is a detailed payload, decode it.
	var payload protoutil.SimpleMessage
	if enc.FullDetails != nil {
		var d types.DynamicAny
		err := types.UnmarshalAny(enc.FullDetails, &d)
		if err != nil {
			// It's OK if we can't decode. We'll use
			// the opaque type below.
			log.Warningf(context.Background(), "error while unmarshalling wrapper error: %+v", err)
		} else {
			payload = d.Message
		}
	}

	// Do we have a wrapper decoder for this?
	if decoder, ok := decoders[enc.TypeName]; ok {
		// Yes, use it.
		genErr := decoder(cause, enc.MessagePrefix, enc.ReportablePayload, payload)
		if genErr != nil {
			// Decoding succeeded. Use this.
			return genErr
		}
		// Decoding failed, we'll drop through to opaqueWrapper{} below.
	}

	// Otherwise, preserve all details about the original object.
	return &opaqueWrapper{
		cause:       cause,
		prefix:      enc.MessagePrefix,
		typeName:    enc.TypeName,
		safeDetails: enc.ReportablePayload,
		payload:     enc.FullDetails,
	}
}

// RegisterLeafDecoder can be used to register new leaf error types to
// the library. Registered types will be decoded using their own
// Go type when an error is decoded. Wrappers that have not been
// registered will be decoded using the opaqueLeaf type.
func RegisterLeafDecoder(typeName string, decoder LeafDecoder) {
	leafDecoders[typeName] = decoder
}

// LeafDecoder is to be provided (via RegisterLeafDecoder above)
// by additional wrapper types not yet known to this library.
// A nil return indicates that decoding was not successful.
type LeafDecoder func(msg string, safeDetails []string, payload protoutil.SimpleMessage) error

// registry for RegisterLeafDecoder.
var leafDecoders = map[string]LeafDecoder{}

// RegisterWrapperDecoder can be used to register new wrapper types to
// the library. Registered wrappers will be decoded using their own
// Go type when an error is decoded. Wrappers that have not been
// registered will be decoded using the opaqueWrapper type.
func RegisterWrapperDecoder(typeName string, decoder WrapperDecoder) {
	decoders[typeName] = decoder
}

// WrapperDecoder is to be provided (via RegisterWrapperDecoder above)
// by additional wrapper types not yet known to this library.
// A nil return indicates that decoding was not successful.
type WrapperDecoder func(cause error, msgPrefix string, safeDetails []string, payload protoutil.SimpleMessage) error

// registry for RegisterWrapperType.
var decoders = map[string]WrapperDecoder{}
