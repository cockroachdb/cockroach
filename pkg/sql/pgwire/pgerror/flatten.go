// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgerror

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
)

// Flatten turns any error into a pgerror with fields populated.  As
// the name implies, the details from the chain of causes is projected
// into a single struct. This is useful in at least two places:
//
// - to generate Error objects suitable for 19.1 nodes, which
//   only recognize this type of payload.
// - to generate an error packet on pgwire.
//
// Additionally, this can be used in the remainder of the code
// base when an Error object is expected, until that code
// is updated to use the errors library directly.
//
// Flatten() returns a nil ptr if err was nil to start with.
func Flatten(err error) *Error {
	if err == nil {
		return nil
	}
	resErr := &Error{
		Code:    GetPGCode(err),
		Message: err.Error(),
	}

	// Populate the source field if available.
	if file, line, fn, ok := errors.GetOneLineSource(err); ok {
		resErr.Source = &Error_Source{File: file, Line: int32(line), Function: fn}
	}

	// Populate the details and hints.
	resErr.Hint = errors.FlattenHints(err)
	resErr.Detail = errors.FlattenDetails(err)

	// Populate Keys for backward-compatibility with 19.1.
	// TODO(knz): Remove in 19.3.
	if keys := errors.GetTelemetryKeys(err); len(keys) > 0 {
		// We may lose keys. That's all right, backward compat here is just best effort.
		resErr.TelemetryKey = keys[0]
	}
	// Populate safe strings for backward-compatibility with 19.1.
	// TODO(knz): Remove in 19.3.
	for _, dd := range errors.GetAllSafeDetails(err) {
		for _, d := range dd.SafeDetails {
			resErr.SafeDetail = append(resErr.SafeDetail,
				&Error_SafeDetail{SafeMessage: d})
		}
	}

	// Add a useful error prefix if not already there.
	switch resErr.Code {
	case pgcode.Internal:
		// The string "internal error" clarifies the nature of the error
		// to users, and is also introduced for compatibility with
		// previous CockroachDB versions.
		if !strings.HasPrefix(resErr.Message, InternalErrorPrefix) {
			resErr.Message = InternalErrorPrefix + ": " + resErr.Message
		}

		// If the error flows towards a human user and does not get
		// sent via telemetry, we want to empower the user to
		// file a moderately useful error report. For this purpose,
		// append the innermost stack trace.
		resErr.Detail += getInnerMostStackTraceAsDetail(err)

	case pgcode.SerializationFailure:
		// The string "restart transaction" is asserted by test code. This
		// can be changed if/when test code learns to use the 40001 code
		// (or the errors library) instead.
		//
		// TODO(knz): investigate whether 3rd party frameworks parse this
		// string instead of using the pg code to determine whether to
		// retry.
		if !strings.HasPrefix(resErr.Message, TxnRetryMsgPrefix) {
			resErr.Message = TxnRetryMsgPrefix + ": " + resErr.Message
		}
	}

	return resErr
}

func getInnerMostStackTraceAsDetail(err error) string {
	if c := errors.UnwrapOnce(err); c != nil {
		s := getInnerMostStackTraceAsDetail(c)
		if s != "" {
			return s
		}
	}
	// Fall through: there is no stack trace so far.
	if st := errors.GetReportableStackTrace(err); st != nil {
		var t bytes.Buffer
		t.WriteString("stack trace:\n")
		for i := len(st.Frames) - 1; i >= 0; i-- {
			f := st.Frames[i]
			fmt.Fprintf(&t, "%s:%d: %s()\n", f.Filename, f.Lineno, f.Function)
		}
		return t.String()
	}
	return ""
}

// InternalErrorPrefix is prepended on internal errors.
const InternalErrorPrefix = "internal error"

// TxnRetryMsgPrefix is the prefix inserted in an error message when flattened
const TxnRetryMsgPrefix = "restart transaction"

// GetPGCode retrieves the error code for an error.
func GetPGCode(err error) string {
	return GetPGCodeInternal(err, ComputeDefaultCode)
}
