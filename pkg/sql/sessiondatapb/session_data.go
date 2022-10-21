// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessiondatapb

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// GetFloatPrec computes a precision suitable for a call to
// strconv.FormatFloat() or for use with '%.*g' in a printf-like
// function.
func (c DataConversionConfig) GetFloatPrec(typ *types.T) int {
	// The user-settable parameter ExtraFloatDigits indicates the number of digits
	// to be used to format the float value. PostgreSQL combines this with %g.
	// If ExtraFloatDigits is less than or equal to zero, the formula is
	// <type>_DIG + extra_float_digits, where <type> is either FLT (float4) or DBL
	// (float8).
	// If ExtraFloatDigits is greater than zero, then the "shortest precise
	// format" is used. The Go formatter uses the special value -1 for this and
	// activates a separate path in the formatter.
	// NB: In previous versions, only the value "3" would result in the shortest
	// precise format. This change in behavior was made in PostgreSQL 12.
	// See https://github.com/postgres/postgres/commit/02ddd499322ab6f2f0d58692955dc9633c2150fc.
	if c.ExtraFloatDigits > 0 {
		return -1
	}

	// Go does not expose FLT_DIG or DBL_DIG, so we use the standard literal
	// constants for 32-bit and 64-bit floats.
	const StdFloatDigits = 6
	const StdDoubleDigits = 15

	nDigits := StdDoubleDigits + c.ExtraFloatDigits
	if typ.Width() == 32 {
		nDigits = StdFloatDigits + c.ExtraFloatDigits
	}
	if nDigits < 1 {
		// Ensure the value is clamped at 1: printf %g does not allow
		// values lower than 1. PostgreSQL does this too.
		nDigits = 1
	}
	return int(nDigits)
}

func (m VectorizeExecMode) String() string {
	switch m {
	case VectorizeOn, VectorizeUnset:
		return "on"
	case VectorizeExperimentalAlways:
		return "experimental_always"
	case VectorizeOff:
		return "off"
	default:
		return fmt.Sprintf("invalid (%d)", m)
	}
}

// VectorizeExecModeFromString converts a string into a VectorizeExecMode.
// False is returned if the conversion was unsuccessful.
func VectorizeExecModeFromString(val string) (VectorizeExecMode, bool) {
	var m VectorizeExecMode
	switch strings.ToUpper(val) {
	case "ON":
		m = VectorizeOn
	case "EXPERIMENTAL_ALWAYS":
		m = VectorizeExperimentalAlways
	case "OFF":
		m = VectorizeOff
	default:
		return 0, false
	}
	return m, true
}

// User retrieves the current user.
func (s *SessionData) User() username.SQLUsername {
	return s.UserProto.Decode()
}

// SystemIdentity retrieves the session's system identity.
// (Identity presented by the client prior to identity mapping.)
func (s *LocalOnlySessionData) SystemIdentity() username.SQLUsername {
	return s.SystemIdentityProto.Decode()
}
