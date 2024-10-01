// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigbounds

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/redact"
)

// Field corresponds to a field in the SpanConfig.
type Field interface {
	fmt.Stringer
	redact.SafeFormatter

	// FieldValue provides access to the field in a SpanConfig as
	// a value which can be displayed.
	FieldValue(c *roachpb.SpanConfig) Value

	// FieldBound provides access to the corresponding spanConfigBound implied by
	// a SpanConfigBounds as a value which can be displayed.
	FieldBound(b *Bounds) ValueBounds
}

// field is a thin interface used to reduce some ceremony around extracting
// the typed value for a field.
type field[T any] interface {
	Field
	fieldValue(c *roachpb.SpanConfig) *T
}

// Note that the order of fields in this slice matters. When clamping, fields
// will be clamped in this order. Order matters for fields like constraints and
// voterConstraints which consult other fields to determine the appropriate
// value.
var fields = [config.NumFields]Field{
	rangeMinBytes,
	rangeMaxBytes,
	globalReads,
	numVoters,
	numReplicas,
	gcTTLSeconds,
	constraints,
	voterConstraints,
	leasePreferences,
}

const (
	rangeMaxBytes    = int64Field(config.RangeMaxBytes)
	rangeMinBytes    = int64Field(config.RangeMinBytes)
	globalReads      = boolField(config.GlobalReads)
	numReplicas      = int32Field(config.NumReplicas)
	numVoters        = int32Field(config.NumVoters)
	gcTTLSeconds     = int32Field(config.GCTTL)
	constraints      = constraintsConjunctionField(config.Constraints)
	voterConstraints = constraintsConjunctionField(config.VoterConstraints)
	leasePreferences = leasePreferencesField(config.LeasePreferences)
)
