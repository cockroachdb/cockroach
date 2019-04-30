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

package errors

import "github.com/cockroachdb/cockroach/pkg/errors/errbase"

// UnwrapOnce accesses the direct cause of the error if any, otherwise
// returns nil.
var UnwrapOnce func(err error) error = errbase.UnwrapOnce

// UnwrapAll accesses the root cause object of the error.
// If the error has no cause (leaf error), it is returned directly.
var UnwrapAll func(err error) error = errbase.UnwrapAll

// EncodedError is a protobuf-encodable type that encodes
// all the details of an error.
type EncodedError = errbase.EncodedError

// EncodeError encodes an error.
var EncodeError func(err error) EncodedError = errbase.EncodeError

// DecodeError decodes an error.
var DecodeError func(enc EncodedError) error = errbase.DecodeError

// SafeDetailer is an interface that can be implemented by errors that
// can provide PII-free additional strings suitable for reporting or
// telemetry.
type SafeDetailer = errbase.SafeDetailer

// GetAllSafeDetails collects the safe details from the given error object
// and all its causes.
var GetAllSafeDetails func(err error) []SafeDetailPayload = errbase.GetAllSafeDetails

// GetSafeDetails collects the safe details from the given error
// object. If it is a wrapper, only the details from the wrapper are
// returned.
var GetSafeDetails func(err error) (payload SafeDetailPayload) = errbase.GetSafeDetails

// SafeDetailPayload captures the safe strings for one
// level of wrapping.
type SafeDetailPayload = errbase.SafeDetailPayload

// RegisterLeafDecoder can be used to register new leaf error types to
// the library.
var RegisterLeafDecoder func(typeName TypeName, decoder LeafDecoder) = errbase.RegisterLeafDecoder

// TypeName is the name of an error type as used by the
// encoding/decoding algorithms and error markers.
type TypeName = errbase.TypeName

// LeafDecoder is the function type to decode leaf errors.
type LeafDecoder = errbase.LeafDecoder

// RegisterWrapperDecoder can be used to register new wrapper types to
// the library.
var RegisterWrapperDecoder func(typeName TypeName, decoder WrapperDecoder) = errbase.RegisterWrapperDecoder

// WrapperDecoder is the function type to decode wrapper errors.
type WrapperDecoder = errbase.WrapperDecoder

// RegisterLeafEncoder can be used to register new leaf error types to
// the library.
var RegisterLeafEncoder func(typeName TypeName, encoder LeafEncoder) = errbase.RegisterLeafEncoder

// LeafEncoder is the function type to encode leaf errors.
type LeafEncoder = errbase.LeafEncoder

// RegisterWrapperEncoder can be used to register new wrapper types to
// the library.
var RegisterWrapperEncoder func(typeName TypeName, encoder WrapperEncoder) = errbase.RegisterWrapperEncoder

// WrapperEncoder is the function type to encode wrapper errors.
type WrapperEncoder = errbase.WrapperEncoder
