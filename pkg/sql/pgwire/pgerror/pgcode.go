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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
)

// WithCandidateCode decorates the error with a candidate postgres
// error code. It is called "candidate" because the code is only used
// by GetPGCode() below conditionally.
// The code is considered PII-free and is thus reportable.
func WithCandidateCode(err error, code pgcode.Code) error {
	if err == nil {
		return nil
	}

	return &withCandidateCode{cause: err, code: code.String()}
}

// HasCandidateCode returns tue iff the error or one of its causes
// has a candidate pg error code.
func HasCandidateCode(err error) bool {
	return errors.HasType(err, (*withCandidateCode)(nil))
}

// GetPGCodeInternal retrieves a code for the error. It operates by
// combining the inner (cause) code and the code at the current level,
// at each level of cause.
//
// - at each level:
//
//   - if there is a candidate code at that level, that is used;
//   - otherwise, it calls computeDefaultCode().
//     if the function returns an empty string,
//     UncategorizedError is used.
//     An example implementation for computeDefaultCode is provided below.
//
// - after that, it combines the code computed already for the cause
//   (inner) and the new code just computed at the current level (outer)
//   as follows:
//
//   - if the outer code is uncategorized, the inner code is kept no
//     matter what.
//   - if the outer code has the special XX prefix, that is kept.
//     (The "XX" prefix signals importance in the pg code hierarchy.)
//   - if the inner code is not uncategorized, it is retained.
//   - otherwise the outer code is retained.
//
// This function should not be used directly. It is only exported
// for use in testing code. Use GetPGCode() instead.
func GetPGCodeInternal(
	err error, computeDefaultCode func(err error) (code pgcode.Code),
) (code pgcode.Code) {
	code = pgcode.Uncategorized
	if c, ok := err.(*withCandidateCode); ok {
		code = pgcode.MakeCode(c.code)
	} else if newCode := computeDefaultCode(err); newCode.String() != "" {
		code = newCode
	}

	if c := errors.UnwrapOnce(err); c != nil {
		innerCode := GetPGCodeInternal(c, computeDefaultCode)
		code = combineCodes(innerCode, code)
	}

	return code
}

// ComputeDefaultCode looks at the current error object
// (not its causes) and returns:
// - the existing code for Error instances
// - SerializationFailure for roachpb retry errors that can be reported to clients
// - StatementCompletionUnknown for ambiguous commit errors
// - InternalError for assertion failures
// - FeatureNotSupportedError for unimplemented errors.
//
// It is not meant to be used directly - it is only exported
// for use by test code. Use GetPGCode() instead.
func ComputeDefaultCode(err error) pgcode.Code {
	switch e := err.(type) {
	// If there was already a pgcode in the cause, use that.
	case *Error:
		return pgcode.MakeCode(e.Code)
	// Special roachpb errors get a special code.
	case ClientVisibleRetryError:
		return pgcode.SerializationFailure
	case ClientVisibleAmbiguousError:
		return pgcode.StatementCompletionUnknown
	}

	if errors.IsAssertionFailure(err) {
		return pgcode.Internal
	}
	if errors.IsUnimplementedError(err) {
		return pgcode.FeatureNotSupported
	}
	return pgcode.Code{}
}

// ClientVisibleRetryError mirrors roachpb.ClientVisibleRetryError but
// is defined here to avoid an import cycle.
type ClientVisibleRetryError interface {
	ClientVisibleRetryError()
}

// ClientVisibleAmbiguousError mirrors
// roachpb.ClientVisibleAmbiguousError but is defined here to avoid an
// import cycle.
type ClientVisibleAmbiguousError interface {
	ClientVisibleAmbiguousError()
}

// combineCodes combines the inner and outer codes.
func combineCodes(innerCode, outerCode pgcode.Code) pgcode.Code {
	if outerCode == pgcode.Uncategorized {
		return innerCode
	}
	if strings.HasPrefix(outerCode.String(), "XX") {
		return outerCode
	}
	if innerCode != pgcode.Uncategorized {
		return innerCode
	}
	return outerCode
}
