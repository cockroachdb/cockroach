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

package pgerror

// EncodableError abstracts an error that can fit in a
// sqlerror.AnyErrorContainer and is suitable for protobuf encoding.
// This includes pgerror.Error (the error defined in this package)
// and a few other errors in roachpb.
//
// We can't actually define the type here, because roachpb has an
// indirect dependency on pgerror and we'd have a dependency cycle
// otherwise.
type EncodableError = interface{}

// Encoder is used with EncodeErrorInternal. It allows the caller to provide the
// code for converting errors to encodable objects.
type Encoder interface {
	DefaultCode(err EncodableError, defaultCode string) EncodableError
	Message(err EncodableError, msg string) EncodableError
	Detail(err EncodableError, detail string) EncodableError
	Hint(err EncodableError, hint string) EncodableError
	TelemetryKey(err EncodableError, key string) EncodableError
	ErrorSource(err EncodableError, src *Error_Source) EncodableError
	SafeDetail(err EncodableError, detail *SafeDetailPayload) EncodableError
	UnknownErrorPayload(err EncodableError, payload string) EncodableError
	InternalError(err EncodableError, intErrCode string) EncodableError
	OtherError(err error) EncodableError
}

// EncodeErrorInternal walks an error chain and uses the Encoder interface to
// encode the error "chain". Do not use this directly; use sqlerror.EncodeError
// instead.
func EncodeErrorInternal(err error, encoder Encoder) EncodableError {
	switch e := err.(type) {
	// Wrapped error types.
	case *withMessage:
		return encoder.Message(EncodeErrorInternal(e.cause, encoder), e.msg)
	case *withDefaultCode:
		return encoder.DefaultCode(EncodeErrorInternal(e.cause, encoder), e.code)
	case *withHint:
		return encoder.Hint(EncodeErrorInternal(e.cause, encoder), e.hint)
	case *withDetail:
		return encoder.Detail(EncodeErrorInternal(e.cause, encoder), e.detail)
	case *withTelemetryKey:
		return encoder.TelemetryKey(EncodeErrorInternal(e.cause, encoder), e.key)
	case *withSource:
		return encoder.ErrorSource(EncodeErrorInternal(e.cause, encoder), e.source)
	case *withSafeDetail:
		return encoder.SafeDetail(EncodeErrorInternal(e.cause, encoder), e.detail)
	case *withUnknownErrorPayload:
		return encoder.UnknownErrorPayload(EncodeErrorInternal(e.cause, encoder), e.payloadType)
	case *withInternalError:
		return encoder.InternalError(EncodeErrorInternal(e.cause, encoder), e.internalErrorCode)
	default:
		return encoder.OtherError(err)
	}
}
