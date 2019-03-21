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

// The following functions are injected as dependency when package sqlerror
// is initialized.
var (
	ConvertOtherError          func(err error) EncodableError
	ConvertDefaultCode         func(err EncodableError, defaultCode string) EncodableError
	ConvertMessage             func(err EncodableError, msg string) EncodableError
	ConvertDetail              func(err EncodableError, detail string) EncodableError
	ConvertHint                func(err EncodableError, hint string) EncodableError
	ConvertTelemetryKey        func(err EncodableError, key string) EncodableError
	ConvertErrorSource         func(err EncodableError, src *Error_Source) EncodableError
	ConvertSafeDetail          func(err EncodableError, detail *SafeDetailPayload) EncodableError
	ConvertUnknownErrorPayload func(err EncodableError, payload string) EncodableError
	ConvertInternalError       func(err EncodableError, intErrCode string) EncodableError
)

// EncodeErrorInternal walks an error chain and uses the functions
// above to encode the error. Do not use this directly; use
// sqlerror.EncodeError instead.
func EncodeErrorInternal(err error) EncodableError {
	switch e := err.(type) {
	// Wrapped error types.
	case *withMessage:
		return ConvertMessage(EncodeErrorInternal(e.cause), e.msg)
	case *withDefaultCode:
		return ConvertDefaultCode(EncodeErrorInternal(e.cause), e.code)
	case *withHint:
		return ConvertHint(EncodeErrorInternal(e.cause), e.hint)
	case *withDetail:
		return ConvertDetail(EncodeErrorInternal(e.cause), e.detail)
	case *withTelemetryKey:
		return ConvertTelemetryKey(EncodeErrorInternal(e.cause), e.key)
	case *withSource:
		return ConvertErrorSource(EncodeErrorInternal(e.cause), e.source)
	case *withSafeDetail:
		return ConvertSafeDetail(EncodeErrorInternal(e.cause), e.detail)
	case *withUnknownErrorPayload:
		return ConvertUnknownErrorPayload(EncodeErrorInternal(e.cause), e.payloadType)
	case *withInternalError:
		return ConvertInternalError(EncodeErrorInternal(e.cause), e.internalErrorCode)
	default:
		return ConvertOtherError(err)
	}
}
