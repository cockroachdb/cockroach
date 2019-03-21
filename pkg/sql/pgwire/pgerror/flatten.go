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
//

package pgerror

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// Flatten turns any error into a pgerror with fields populated.
// Returns a nil ptr if err was nil to start with.
//
// The string slice is a list of telemetry keys (including
// the one mentioned in the Error itself).
func Flatten(err error) (*Error, []SafeDetailPayload, []string) {
	if err == nil {
		return nil, nil, nil
	}
	pgErr := Error{
		Code:    CodeUncategorizedError,
		Message: err.Error(),
	}
	resErr, details, keys := doFlatten(1, &pgErr, nil, nil, err)

	if resErr.Code == CodeInternalError {
		if !strings.HasPrefix(resErr.Message, InternalErrorPrefix) {
			// The internal error prefix wasn't there already. Add it.
			resErr.Message = InternalErrorPrefix + resErr.Message
		}
	}
	return resErr, details, keys
}

// doFlatten does a post-order traversal of the details.
// At every level, we add details after those details already
// added by the inner (lower) levels.
func doFlatten(
	depth int, dstErr *Error, dstDetails []SafeDetailPayload, dstKeys []string, err error,
) (*Error, []SafeDetailPayload, []string) {
	if causer, ok := err.(Causer); ok {
		dstErr, dstDetails, dstKeys = doFlatten(depth+1, dstErr, dstDetails, dstKeys, causer.Cause())
	}

	// Add every additional detail.
	// Note: we do not need to process WrapMessageError,
	// as the prefix message was already added by Flatten()
	// via the Error() call.
	switch e := err.(type) {
	case *Error:
		// Keep everything except the message, which is constructed by
		// Flatten() above.
		newErr := *e
		newErr.Message = dstErr.Message
		*dstErr = newErr
		if dstErr.TelemetryKey != "" {
			dstKeys = append(dstKeys, dstErr.TelemetryKey)
		}

	case *withInternalError:
		code := e.internalErrorCode
		if code == "" {
			code = CodeInternalError
		}
		dstErr.Code = code

	case *withUnknownErrorPayload:
		dstErr.Code = CodeInternalError
		dstDetails = append(dstDetails, SafeDetailPayload{
			SafeMessage: fmt.Sprintf("unknown payload: %s", e.payloadType),
		})

	case *withDefaultCode:
		if dstErr.Code == "" || dstErr.Code == CodeUncategorizedError {
			dstErr.Code = e.code
		}

	case *withTelemetryKey:
		if dstErr.TelemetryKey == "" {
			dstErr.TelemetryKey = e.key
		}
		dstKeys = append(dstKeys, e.key)

	case *withDetail:
		var buf bytes.Buffer
		if dstErr.Detail != "" {
			fmt.Fprintf(&buf, "%s\n", dstErr.Detail)
		}
		fmt.Fprintf(&buf, "%s", e.detail)
		dstErr.Detail = buf.String()

	case *withHint:
		var buf bytes.Buffer
		if dstErr.Hint != "" {
			fmt.Fprintf(&buf, "%s\n", dstErr.Hint)
		}
		fmt.Fprintf(&buf, "%s", e.hint)
		dstErr.Hint = buf.String()

	case *withSafeDetail:
		dstDetails = append(dstDetails, *e.detail)

	case *withSource:
		if dstErr.Source == nil {
			dstErr.Source = e.source
		}
		if e.source != nil {
			src := e.source
			dstDetails = append(dstDetails, SafeDetailPayload{
				SafeMessage: fmt.Sprintf("%s:%d: in %s()", src.File, src.Line, src.Function),
			})
		}

	default:
		// Special roachpb errors get a special code.
		switch err.(type) {
		case clientVisibleRetryError:
			dstErr.Code = CodeSerializationFailureError
			dstErr.Message = fmt.Sprintf("%s: %s", TxnRetryMsgPrefix, dstErr.Message)
		case clientVisibleAmbiguousError:
			dstErr.Code = CodeStatementCompletionUnknownError
		}

		dstDetails = append(dstDetails, SafeDetailPayload{
			SafeMessage: log.Redact(err),
		})

		// If a stack trace was available in the original
		// non-Error error (e.g. when constructed via errors.Wrap),
		// try to get useful information from it.
		if se, ok := err.(StackTracer); ok {
			tr := se.StackTrace()

			if dstErr.Source == nil && len(tr) > 0 {
				// Assemble a source information from scratch using
				// the provided stack trace.
				line, _ := strconv.Atoi(fmt.Sprintf("%d", tr[0]))
				dstErr.Source = &Error_Source{
					File:     fmt.Sprintf("%s", tr[0]),
					Line:     int32(line),
					Function: fmt.Sprintf("%n", tr[0]),
				}
			}

			if len(tr) > 0 {
				dstDetails = append(dstDetails, SafeDetailPayload{
					SafeMessage:       fmt.Sprintf("%v", tr[0]),
					EncodedStackTrace: fmt.Sprintf("%+v", tr),
				})
			}
		}
	}

	return dstErr, dstDetails, dstKeys
}

// StackTracer is a helper interface to access extra stacktraces from
// errors constructed with errors.Wrapf().
type StackTracer interface {
	StackTrace() errors.StackTrace
}
