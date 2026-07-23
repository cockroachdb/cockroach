// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package explain

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// Flags are modifiers for EXPLAIN.
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
	// If OnlyShape is true, we hide fields that could be different between 2
	// plans that otherwise have exactly the same shape, like estimated row count.
	// This is used for EXPLAIN (SHAPE), which is used for the statement-bundle
	// debug tool.
	OnlyShape bool
	// RedactValues is similar to HideValues but indicates that we should use
	// redaction markers instead of underscores. Used by EXPLAIN (REDACT).
	RedactValues bool

	// Flags to hide various fields for testing purposes.
	Deflake DeflakeFlags
	// ShowPolicyInfo indicates that row-level security policy information is
	// shown.
	ShowPolicyInfo bool
}

// DeflakeFlags control hiding of various field values. They are used to
// guarantee deterministic results for testing purposes.
type DeflakeFlags uint8

const (
	// DeflakeDistribution hides the value of the "distribution" field.
	DeflakeDistribution DeflakeFlags = (1 << iota)

	// DeflakeVectorized hides the value of the "vectorized" field.
	DeflakeVectorized

	// DeflakeNodes hides cluster nodes involved.
	DeflakeNodes

	// DeflakeVolatile hides any values that can vary from one query run to the
	// other, even without changes to the configuration or data distribution (e.g.
	// timings).
	DeflakeVolatile
)

const (
	// DeflakeAll has all redact flags set.
	DeflakeAll DeflakeFlags = DeflakeDistribution | DeflakeVectorized | DeflakeNodes | DeflakeVolatile
)

// HasAny returns true if the receiver has any of the given deflake flags set.
func (f DeflakeFlags) HasAny(flags DeflakeFlags) bool {
	return (f & flags) != 0
}

// MakeFlags crates Flags from ExplainOptions.
func MakeFlags(options *tree.ExplainOptions) Flags {
	var f Flags
	if options.Flags[tree.ExplainFlagVerbose] {
		f.Verbose = true
		f.ShowPolicyInfo = true
	}
	if options.Flags[tree.ExplainFlagTypes] {
		f.Verbose = true
		f.ShowPolicyInfo = true
		f.ShowTypes = true
	}
	if options.Flags[tree.ExplainFlagShape] {
		f.HideValues = true
		f.OnlyShape = true
		f.Deflake = DeflakeAll
	}
	if options.Flags[tree.ExplainFlagRedact] {
		f.RedactValues = true
	}
	return f
}
