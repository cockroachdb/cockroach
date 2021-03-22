// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package explain

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// Flags are modifiers for EXPLAIN (PLAN).
type Flags struct {
	// Verbose indicates that more metadata is shown, and plan columns and
	// ordering are shown.
	Verbose bool
	// ShowTypes indicates that the types of columns are shown.
	// If ShowTypes is true, then Verbose is also true.
	ShowTypes bool
	// If HideValues is true, we hide fields that may contain values from the
	// query (e.g. spans). Used internally for the plan visible in the UI.
	// If HideValues is true, then Verbose must be false.
	HideValues bool

	// Redaction control (for testing purposes).
	Redact RedactFlags
}

// RedactFlags control the redacting of various field values. They are used to
// guarantee deterministic results for testing purposes.
type RedactFlags uint8

const (
	// RedactDistribution hides the value of the "distribution" field.
	RedactDistribution RedactFlags = (1 << iota)

	// RedactVectorized hides the value of the "vectorized" field.
	RedactVectorized

	// RedactNodes hides cluster nodes involved.
	RedactNodes

	// RedactVolatile hides any values that can vary from one query run to the
	// other, even without changes to the configuration or data distribution (e.g.
	// timings).
	RedactVolatile
)

const (
	// RedactAll has all redact flags set.
	RedactAll RedactFlags = RedactDistribution | RedactVectorized | RedactNodes | RedactVolatile
)

// Has returns true if the receiver has the given flag set.
func (f RedactFlags) Has(flag RedactFlags) bool {
	return (f & flag) != 0
}

// MakeFlags crates Flags from ExplainOptions.
func MakeFlags(options *tree.ExplainOptions) Flags {
	var f Flags
	if options.Flags[tree.ExplainFlagVerbose] {
		f.Verbose = true
	}
	if options.Flags[tree.ExplainFlagTypes] {
		f.Verbose = true
		f.ShowTypes = true
	}
	return f
}
