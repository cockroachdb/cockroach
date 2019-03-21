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
	"github.com/pkg/errors"
)

// withMessage adds a message prefix.
type withMessage struct {
	cause error
	msg   string
}

// withCode decorates an error with a pg error code.
type withCode struct {
	cause error
	code  string
}

// withHint adds a pg error hint.
type withHint struct {
	cause error
	hint  string
}

// withDetail adds a pg error detail.
type withDetail struct {
	cause  error
	detail string
}

// withSource adds a pg error source  .
type withSource struct {
	cause  error
	source *pgerror.Error_Source
}

// withTelemetryKey adds a key for telemetry.
type withTelemetryKey struct {
	cause error
	key   string
}

// withSafeDetail adds a reportable (PII-free) layer of safe details.
type withSafeDetail struct {
	cause  error
	detail *SafeDetailPayload
}

// withUnknownErrorPayload indicates that an error wrapper received
// from the network is not understood on this host and was
// discarded. It is a sort of internal error and gets turned into a
// pgerror.Error internal error when flattened.
type withUnknownErrorPayload struct {
	cause       error
	payloadType string
}

// withInternalError is a general purpose internal error marker and
// gets turned into a pgerror.Error internal error when flattened.
type withInternalError struct {
	cause error
	// internalErrorCode decides the final code to use for the internal
	// error. This is going to be CodeInternalError in most cases, but
	// can be some of the others too (e.g. XX001 corrupted data).
	internalErrorCode string
}

// stackTrace is a helper interface to extra stacktraces from
// errors constructed with errors.Wrapf().
type stackTracer interface {
	StackTrace() errors.StackTrace
}

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
		return baseErrs.New(t.OtherError.Message)

	default:
		return &withInternalError{
			cause: errors.Errorf("unknown error type received: %T", e.Detail),
		}
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
	case *ErrorWrapperPayload_WithCode:
		if p.WithCode == nil || p.WithCode.Text == "" {
			return cause
		}
		return &withCode{code: p.WithCode.Text, cause: cause}
	case *ErrorWrapperPayload_WithMessage:
		if p.WithMessage == nil || p.WithMessage.Text == "" {
			return cause
		}
		return &withMessage{msg: p.WithMessage.Text, cause: cause}
	case *ErrorWrapperPayload_WithDetail:
		if p.WithDetail == nil || p.WithDetail.Text == "" {
			return cause
		}
		return &withDetail{detail: p.WithDetail.Text, cause: cause}
	case *ErrorWrapperPayload_WithHint:
		if p.WithHint == nil || p.WithHint.Text == "" {
			return cause
		}
		return &withHint{hint: p.WithHint.Text, cause: cause}
	case *ErrorWrapperPayload_WithTelemetryKey:
		if p.WithTelemetryKey == nil || p.WithTelemetryKey.Text == "" {
			return cause
		}
		return &withTelemetryKey{key: p.WithTelemetryKey.Text, cause: cause}
	case *ErrorWrapperPayload_WithSource:
		if p.WithSource == nil {
			return cause
		}
		return &withSource{source: p.WithSource, cause: cause}
	case *ErrorWrapperPayload_WithSafeDetail:
		if p.WithSafeDetail == nil {
			return cause
		}
		return &withSafeDetail{detail: p.WithSafeDetail, cause: cause}
	case *ErrorWrapperPayload_WithUnknownErrorPayload:
		if p.WithUnknownErrorPayload == nil {
			return cause
		}
		return &withUnknownErrorPayload{payloadType: p.WithUnknownErrorPayload.Text, cause: cause}

	case *ErrorWrapperPayload_WithInternalError:
		if p.WithInternalError == nil {
			return cause
		}
		return &withInternalError{internalErrorCode: p.WithInternalError.Text, cause: cause}

	default:
		return &withUnknownErrorPayload{
			payloadType: fmt.Sprintf("%T", e.Payload.Payload),
			cause:       cause,
		}
	}
}

func wrapErr(
	cause AnyErrorContainer, payload isErrorWrapperPayload_Payload,
) isAnyErrorContainer_Detail {
	return &AnyErrorContainer_WrappedError{
		WrappedError: &EncodedErrorWrapper{
			Cause:   cause,
			Payload: ErrorWrapperPayload{Payload: payload},
		}}
}

// EncodeError converts a Go error into an AnyErrorContainer.
func EncodeError(err error) (resErr AnyErrorContainer) {
	switch e := err.(type) {
	// Wrapped error types.
	case *withMessage:
		resErr.Detail = wrapErr(EncodeError(e.cause),
			&ErrorWrapperPayload_WithMessage{WithMessage: &TextErrorPayload{Text: e.msg}},
		)
	case *withCode:
		resErr.Detail = wrapErr(EncodeError(e.cause),
			&ErrorWrapperPayload_WithCode{WithCode: &TextErrorPayload{Text: e.code}},
		)
	case *withHint:
		resErr.Detail = wrapErr(EncodeError(e.cause),
			&ErrorWrapperPayload_WithHint{WithHint: &TextErrorPayload{Text: e.hint}},
		)
	case *withDetail:
		resErr.Detail = wrapErr(EncodeError(e.cause),
			&ErrorWrapperPayload_WithDetail{WithDetail: &TextErrorPayload{Text: e.detail}},
		)
	case *withTelemetryKey:
		resErr.Detail = wrapErr(EncodeError(e.cause),
			&ErrorWrapperPayload_WithTelemetryKey{WithTelemetryKey: &TextErrorPayload{Text: e.key}},
		)
	case *withSource:
		resErr.Detail = wrapErr(EncodeError(e.cause),
			&ErrorWrapperPayload_WithSource{WithSource: e.source},
		)
	case *withSafeDetail:
		resErr.Detail = wrapErr(EncodeError(e.cause),
			&ErrorWrapperPayload_WithSafeDetail{WithSafeDetail: e.detail},
		)
	case *withUnknownErrorPayload:
		resErr.Detail = wrapErr(EncodeError(e.cause),
			&ErrorWrapperPayload_WithUnknownErrorPayload{WithUnknownErrorPayload: &TextErrorPayload{Text: e.payloadType}},
		)
	case *withInternalError:
		resErr.Detail = wrapErr(EncodeError(e.cause),
			&ErrorWrapperPayload_WithInternalError{WithInternalError: &TextErrorPayload{Text: e.internalErrorCode}},
		)

		// "Leaf" error types.
	case *pgerror.Error:
		resErr.Detail = &AnyErrorContainer_PGError{PGError: e}
	case *roachpb.UnhandledRetryableError:
		resErr.Detail = &AnyErrorContainer_RetryableTxnError{RetryableTxnError: e}
	case *roachpb.AmbiguousResultError:
		resErr.Detail = &AnyErrorContainer_AmbiguousError{AmbiguousError: e}
	case *roachpb.TransactionRetryWithProtoRefreshError:
		resErr.Detail = &AnyErrorContainer_TxnRefreshError{TxnRefreshError: e}

		// Other errors.
	default:
		if causer, ok := err.(causerI); !ok {
			// Not a causer. Wrap in the general-purpose leaf error type.
			resErr.Detail = &AnyErrorContainer_OtherError{OtherError: &UnencodableError{Message: fmt.Sprintf("%+v", err)}}
		} else {
			// Is a causer.
			errCause := causer.Cause()

			// Extract a message prefix, if any.
			suffixMsg := fmt.Sprintf(": %s", errCause.Error())
			wrappedMsg := err.Error()
			msg := strings.TrimSuffix(wrappedMsg, suffixMsg)
			if len(msg) < len(wrappedMsg) {
				// This is a wrapper message. Handle as above.
				resErr.Detail = wrapErr(EncodeError(errCause),
					&ErrorWrapperPayload_WithMessage{WithMessage: &TextErrorPayload{Text: msg}},
				)
			} else {
				// We don't really know what to do with this error type,
				// however we can encode its cause.
				resErr = EncodeError(errCause)
			}

			// If the error is not a simple error from either the go package
			// "errors" or github.com/pkg/errors, indicate we couldn't
			// encode the error. This may later turn into an internal error.
			if !isSimpleErrorType(err) {
				resErr = AnyErrorContainer{Detail: wrapErr(resErr,
					&ErrorWrapperPayload_WithUnknownErrorPayload{
						WithUnknownErrorPayload: &TextErrorPayload{Text: fmt.Sprintf("%T", err)},
					},
				)}
			}
		}
	}

	// If there was a stack trace in the wrapper at the current level, append it to the
	// resulting encoding, so that it is preserved on the other side.
	if e, ok := err.(stackTracer); ok {
		tr := e.StackTrace()
		if len(tr) > 0 {
			line, _ := strconv.Atoi(fmt.Sprintf("%d", tr[0]))

			// Add a source payload. This can be communicated to a SQL client in
			// the final pgerror.Error object.
			resErr = AnyErrorContainer{Detail: wrapErr(resErr,
				&ErrorWrapperPayload_WithSource{
					WithSource: &pgerror.Error_Source{
						File:     fmt.Sprintf("%s", tr[0]),
						Line:     int32(line),
						Function: fmt.Sprintf("%n", tr[0]),
					}})}

			// Add a safe detail payload. This can be reported if the error
			// turns into an unexpected internal error.
			resErr = AnyErrorContainer{Detail: wrapErr(resErr,
				&ErrorWrapperPayload_WithSafeDetail{
					WithSafeDetail: &SafeDetailPayload{
						SafeMessage:       fmt.Sprintf("%v", tr[0]),
						EncodedStackTrace: fmt.Sprintf("%+v", tr),
					}})}
		}
	}

	return resErr
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
