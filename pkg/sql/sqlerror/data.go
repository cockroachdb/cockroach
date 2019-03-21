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

package sqlerror

import (
	baseErrs "errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// GetError converts an AnyErrorContainer to a Go error.
func (e *AnyErrorContainer) GetError() error {
	if e == nil {
		return nil
	}
	switch t := e.Detail.(type) {
	case *AnyErrorContainer_WrappedError:
		return t.WrappedError.GetError()
	case *AnyErrorContainer_PGError:
		return t.PGError
	case *AnyErrorContainer_RetryableTxnError:
		return t.RetryableTxnError
	case *AnyErrorContainer_AmbiguousError:
		return t.AmbiguousError
	case *AnyErrorContainer_TxnRefreshError:
		return t.TxnRefreshError
	case *AnyErrorContainer_OtherError:
		return baseErrs.New(t.OtherError)

	default:
		return pgerror.NewAssertionErrorf("unknown error type received: %T", e.Detail)
	}
}

// GetError converts an EncodedErrorWrapper to a Go error.
func (e *EncodedErrorWrapper) GetError() error {
	if e == nil {
		return nil
	}
	cause := e.Cause.GetError()
	if cause == nil {
		return nil
	}
	switch p := e.Payload.Payload.(type) {
	case *ErrorWrapperPayload_DefaultCode:
		return pgerror.WithDefaultCode(cause, p.DefaultCode)
	case *ErrorWrapperPayload_Message:
		return pgerror.WithMessage(cause, p.Message)
	case *ErrorWrapperPayload_Detail:
		return pgerror.WithDetail(cause, p.Detail)
	case *ErrorWrapperPayload_Hint:
		return pgerror.WithHint(cause, p.Hint)
	case *ErrorWrapperPayload_TelemetryKey:
		return pgerror.WithTelemetryKey(p.TelemetryKey, cause)
	case *ErrorWrapperPayload_Source:
		return pgerror.WithErrorSource(cause, p.Source)
	case *ErrorWrapperPayload_SafeDetail:
		return pgerror.WithSafeDetailPayload(cause, p.SafeDetail)
	case *ErrorWrapperPayload_UnknownErrorPayload:
		return pgerror.NewUnknownPayloadWrapper(cause, p.UnknownErrorPayload)
	case *ErrorWrapperPayload_InternalError:
		return pgerror.NewInternalErrorWrapper(cause, p.InternalError)

	default:
		return pgerror.NewUnknownPayloadWrapper(cause, fmt.Sprintf("%T", e.Payload.Payload))
	}
}

// EncodeError converts a Go error to an encodable error.
func EncodeError(err error) AnyErrorContainer {
	return AnyErrorContainer{
		Detail: pgerror.EncodeErrorInternal(err).(isAnyErrorContainer_Detail),
	}
}

func init() {
	pgerror.ConvertDefaultCode = func(err pgerror.EncodableError, defaultCode string) pgerror.EncodableError {
		return wrapErr(err, &ErrorWrapperPayload_DefaultCode{DefaultCode: defaultCode})
	}
	pgerror.ConvertMessage = func(err pgerror.EncodableError, msg string) pgerror.EncodableError {
		return wrapErr(err, &ErrorWrapperPayload_Message{Message: msg})
	}
	pgerror.ConvertDetail = func(err pgerror.EncodableError, detail string) pgerror.EncodableError {
		return wrapErr(err, &ErrorWrapperPayload_Detail{Detail: detail})
	}
	pgerror.ConvertHint = func(err pgerror.EncodableError, hint string) pgerror.EncodableError {
		return wrapErr(err, &ErrorWrapperPayload_Hint{Hint: hint})
	}
	pgerror.ConvertTelemetryKey = func(err pgerror.EncodableError, key string) pgerror.EncodableError {
		return wrapErr(err, &ErrorWrapperPayload_TelemetryKey{TelemetryKey: key})
	}
	pgerror.ConvertErrorSource = func(err pgerror.EncodableError, src *pgerror.Error_Source) pgerror.EncodableError {
		return wrapErr(err, &ErrorWrapperPayload_Source{Source: src})
	}
	pgerror.ConvertSafeDetail = func(err pgerror.EncodableError, detail *pgerror.SafeDetailPayload) pgerror.EncodableError {
		return wrapErr(err, &ErrorWrapperPayload_SafeDetail{SafeDetail: detail})
	}
	pgerror.ConvertUnknownErrorPayload = func(err pgerror.EncodableError, payloadType string) pgerror.EncodableError {
		return wrapErr(err, &ErrorWrapperPayload_UnknownErrorPayload{UnknownErrorPayload: payloadType})
	}
	pgerror.ConvertInternalError = func(err pgerror.EncodableError, intErrCode string) pgerror.EncodableError {
		return wrapErr(err, &ErrorWrapperPayload_InternalError{InternalError: intErrCode})
	}

	pgerror.ConvertOtherError = func(err error) pgerror.EncodableError {
		var detail isAnyErrorContainer_Detail
		switch e := err.(type) {
		// Encodable "leaf" error types.
		case *pgerror.Error:
			detail = &AnyErrorContainer_PGError{PGError: e}
		case *roachpb.UnhandledRetryableError:
			detail = &AnyErrorContainer_RetryableTxnError{RetryableTxnError: e}
		case *roachpb.AmbiguousResultError:
			detail = &AnyErrorContainer_AmbiguousError{AmbiguousError: e}
		case *roachpb.TransactionRetryWithProtoRefreshError:
			detail = &AnyErrorContainer_TxnRefreshError{TxnRefreshError: e}

			// Other errors.
		default:
			if causer, ok := err.(pgerror.Causer); !ok {
				// Not a causer. Wrap in the general-purpose leaf error type.
				detail = &AnyErrorContainer_OtherError{OtherError: fmt.Sprintf("%+v", err)}
			} else {
				// Is a causer.
				errCause := causer.Cause()

				// Extract a message prefix, if any.
				suffixMsg := fmt.Sprintf(": %s", errCause.Error())
				wrappedMsg := err.Error()
				msg := strings.TrimSuffix(wrappedMsg, suffixMsg)
				if len(msg) < len(wrappedMsg) {
					// This is a wrapper message. Handle as above.
					detail = wrapErr(pgerror.EncodeErrorInternal(errCause),
						&ErrorWrapperPayload_Message{Message: msg})
				} else {
					// We don't really know what to do with this error type,
					// however we can encode its cause.
					detail = pgerror.EncodeErrorInternal(errCause).(isAnyErrorContainer_Detail)
				}

				// If the error is not a simple error from either the go package
				// "errors" or github.com/pkg/errors, indicate we couldn't
				// encode the error. This may later turn into an internal error.
				if !isSimpleErrorType(err) {
					detail = wrapErr(detail,
						&ErrorWrapperPayload_UnknownErrorPayload{UnknownErrorPayload: fmt.Sprintf("%T", err)})
				}
			}
		}

		// If there was a stack trace in the wrapper at the current level,
		// append it to the resulting encoding, so that it is preserved on
		// the other side.
		if e, ok := err.(pgerror.StackTracer); ok {
			tr := e.StackTrace()
			if len(tr) > 0 {
				line, _ := strconv.Atoi(fmt.Sprintf("%d", tr[0]))

				// Add a source payload. This can be communicated to a SQL client in
				// the final pgerror.Error object.
				detail = wrapErr(detail,
					&ErrorWrapperPayload_Source{
						Source: &pgerror.Error_Source{
							File:     fmt.Sprintf("%s", tr[0]),
							Line:     int32(line),
							Function: fmt.Sprintf("%n", tr[0]),
						}})

				// Add a safe detail payload. This can be reported if the error
				// turns into an unexpected internal error.
				detail = wrapErr(detail,
					&ErrorWrapperPayload_SafeDetail{
						SafeDetail: &pgerror.SafeDetailPayload{
							SafeMessage:       fmt.Sprintf("%v", tr[0]),
							EncodedStackTrace: fmt.Sprintf("%+v", tr),
						}})
			}
		}

		return detail
	}
}

// isSimpleErrorType returns true if the error is coming from Go's
// base errors type or github.com/pkg/errors.
func isSimpleErrorType(err error) bool {
	t := reflect.TypeOf(err)
	// We want the path of the type "inside".
	origT := t
	for {
		switch origT.Kind() {
		case reflect.Array, reflect.Chan, reflect.Map, reflect.Ptr, reflect.Slice:
			origT = origT.Elem()
			continue
		}
		break
	}
	pkgPath := origT.PkgPath()
	return pkgPath == "errors" || strings.HasSuffix(pkgPath, "github.com/pkg/errors")
}

// wrapErr is a helper for the conversion functions above.
func wrapErr(
	cause pgerror.EncodableError, payload isErrorWrapperPayload_Payload,
) isAnyErrorContainer_Detail {
	return &AnyErrorContainer_WrappedError{
		WrappedError: &EncodedErrorWrapper{
			Cause:   AnyErrorContainer{Detail: cause.(isAnyErrorContainer_Detail)},
			Payload: ErrorWrapperPayload{Payload: payload},
		},
	}
}
