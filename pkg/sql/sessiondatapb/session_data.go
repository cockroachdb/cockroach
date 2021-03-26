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

	"github.com/cockroachdb/cockroach/pkg/security"
)

// GetFloatPrec computes a precision suitable for a call to
// strconv.FormatFloat() or for use with '%.*g' in a printf-like
// function.
func (c DataConversionConfig) GetFloatPrec() int {
	// The user-settable parameter ExtraFloatDigits indicates the number
	// of digits to be used to format the float value. PostgreSQL
	// combines this with %g.
	// The formula is <type>_DIG + extra_float_digits,
	// where <type> is either FLT (float4) or DBL (float8).

	// Also the value "3" in PostgreSQL is special and meant to mean
	// "all the precision needed to reproduce the float exactly". The Go
	// formatter uses the special value -1 for this and activates a
	// separate path in the formatter. We compare >= 3 here
	// just in case the value is not gated properly in the implementation
	// of SET.
	if c.ExtraFloatDigits >= 3 {
		return -1
	}

	// CockroachDB only implements float8 at this time and Go does not
	// expose DBL_DIG, so we use the standard literal constant for
	// 64bit floats.
	const StdDoubleDigits = 15

	nDigits := StdDoubleDigits + c.ExtraFloatDigits
	if nDigits < 1 {
		// Ensure the value is clamped at 1: printf %g does not allow
		// values lower than 1. PostgreSQL does this too.
		nDigits = 1
	}
	return int(nDigits)
}

func (f BytesEncodeFormat) String() string {
	switch f {
	case BytesEncodeHex:
		return "hex"
	case BytesEncodeEscape:
		return "escape"
	case BytesEncodeBase64:
		return "base64"
	default:
		return fmt.Sprintf("invalid (%d)", f)
	}
}

// BytesEncodeFormatFromString converts a string into a BytesEncodeFormat.
func BytesEncodeFormatFromString(val string) (_ BytesEncodeFormat, ok bool) {
	switch strings.ToUpper(val) {
	case "HEX":
		return BytesEncodeHex, true
	case "ESCAPE":
		return BytesEncodeEscape, true
	case "BASE64":
		return BytesEncodeBase64, true
	default:
		return -1, false
	}
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

// User retrieves the session user.
func (s *SessionData) User() security.SQLUsername {
	return s.UserProto.Decode()
}
