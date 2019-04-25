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

// UnwrapOnce forwards a definition.
var UnwrapOnce func(err error) error = errbase.UnwrapOnce

// UnwrapAll forwards a definition.
var UnwrapAll func(err error) error = errbase.UnwrapAll

// EncodedError forwards a definition.
type EncodedError = errbase.EncodedError

// EncodeError forwards a definition.
var EncodeError func(err error) EncodedError = errbase.EncodeError

// DecodeError forwards a definition.
var DecodeError func(enc EncodedError) error = errbase.DecodeError

// SafeDetailer forwards a definition.
type SafeDetailer = errbase.SafeDetailer

// GetAllSafeDetails forwards a definition.
var GetAllSafeDetails func(err error) []SafeDetailPayload = errbase.GetAllSafeDetails

// GetSafeDetails forwards a definition.
var GetSafeDetails func(err error) (payload SafeDetailPayload) = errbase.GetSafeDetails

// SafeDetailPayload forwards a definition.
type SafeDetailPayload = errbase.SafeDetailPayload

// RegisterLeafDecoder forwards a definition.
var RegisterLeafDecoder func(typeName TypeKey, decoder LeafDecoder) = errbase.RegisterLeafDecoder

// TypeKey forwards a definition.
type TypeKey = errbase.TypeKey

// LeafDecoder forwards a definition.
type LeafDecoder = errbase.LeafDecoder

// RegisterWrapperDecoder forwards a definition.
var RegisterWrapperDecoder func(typeName TypeKey, decoder WrapperDecoder) = errbase.RegisterWrapperDecoder

// WrapperDecoder forwards a definition.
type WrapperDecoder = errbase.WrapperDecoder

// RegisterLeafEncoder forwards a definition.
var RegisterLeafEncoder func(typeName TypeKey, encoder LeafEncoder) = errbase.RegisterLeafEncoder

// LeafEncoder forwards a definition.
type LeafEncoder = errbase.LeafEncoder

// RegisterWrapperEncoder forwards a definition.
var RegisterWrapperEncoder func(typeName TypeKey, encoder WrapperEncoder) = errbase.RegisterWrapperEncoder

// WrapperEncoder forwards a definition.
type WrapperEncoder = errbase.WrapperEncoder
