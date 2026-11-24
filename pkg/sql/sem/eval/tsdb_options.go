// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// acceptedTSDBQueryOptions enumerates JSONB keys recognized by
// ParseTSDBQueryOptions, used in the unknown-field error message.
var acceptedTSDBQueryOptions = []string{
	"downsampler",
	"interval",
	"derivative",
	"source_aggregator",
	"sources",
}

// ParseTSDBQueryOptions parses the JSONB options blob accepted as
// the fourth argument of crdb_internal.tsdb_query and merges the
// parsed values into q. Missing fields, null fields, and a null blob
// all leave q unchanged for those fields. Parse errors return a
// pgerror with code InvalidParameterValue.
func ParseTSDBQueryOptions(opts json.JSON, q *TimeSeriesQuery) error {
	if opts == nil || opts.Type() == json.NullJSONType {
		return nil
	}
	if opts.Type() != json.ObjectJSONType {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"crdb_internal.tsdb_query options must be a JSON object, got %s",
			opts.Type())
	}
	iter, err := opts.ObjectIter()
	if err != nil {
		return err
	}
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()
		if val.Type() == json.NullJSONType {
			continue // explicit null means use default
		}
		switch key {
		case "downsampler":
			if err := parseAggregator(val, "downsampler", &q.Downsampler); err != nil {
				return err
			}
		case "source_aggregator":
			if err := parseAggregator(val, "source_aggregator", &q.SourceAggregator); err != nil {
				return err
			}
		case "derivative":
			if err := parseDerivative(val, &q.Derivative); err != nil {
				return err
			}
		case "interval":
			if err := parseInterval(val, &q.SampleNanos); err != nil {
				return err
			}
		case "sources":
			if err := parseSources(val, &q.Sources); err != nil {
				return err
			}
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"unknown option %q; accepted options: %s",
				key, strings.Join(acceptedTSDBQueryOptions, ", "))
		}
	}
	// downsampler without interval would silently no-op; reject.
	if q.Downsampler != AggregatorDefault && q.SampleNanos == 0 {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			`"downsampler" requires "interval" to be set`)
	}
	return nil
}

// parseAggregator parses a case-insensitive JSON string into an
// Aggregator. The accept list is the intersection of eval.Aggregator
// and what the TSDB server supports today; FIRST/LAST/VARIANCE are
// rejected here so the user sees a clean InvalidParameterValue rather
// than the TSDB's non-pgcoded "not yet supported" error.
func parseAggregator(v json.JSON, fieldName string, out *Aggregator) error {
	if v.Type() != json.StringJSONType {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"option %q must be a string, got %s", fieldName, v.Type())
	}
	sptr, err := v.AsText()
	if err != nil || sptr == nil {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"option %q must be a string", fieldName)
	}
	switch strings.ToUpper(*sptr) {
	case "AVG":
		*out = AggregatorAvg
	case "SUM":
		*out = AggregatorSum
	case "MAX":
		*out = AggregatorMax
	case "MIN":
		*out = AggregatorMin
	default:
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"invalid %s %q; must be one of AVG, SUM, MAX, MIN",
			fieldName, *sptr)
	}
	return nil
}

// parseDerivative parses a JSON string into a Derivative.
// Case-insensitive.
func parseDerivative(v json.JSON, out *Derivative) error {
	if v.Type() != json.StringJSONType {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"option %q must be a string, got %s", "derivative", v.Type())
	}
	sptr, err := v.AsText()
	if err != nil || sptr == nil {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"option %q must be a string", "derivative")
	}
	switch strings.ToUpper(*sptr) {
	case "NONE":
		*out = DerivativeNone
	case "DERIVATIVE":
		*out = DerivativeFirstOrder
	case "NON_NEGATIVE_DERIVATIVE":
		*out = DerivativeNonNegativeFirstOrder
	default:
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"invalid derivative %q; must be one of NONE, DERIVATIVE, NON_NEGATIVE_DERIVATIVE",
			*sptr)
	}
	return nil
}

// parseInterval parses a JSON string into nanoseconds. Rejects month
// or year components (calendar-dependent length) and requires the
// result be a positive multiple of 10 seconds. Days are treated as
// exactly 24 hours, since TSDB bucketing operates on absolute
// nanoseconds rather than local calendar time.
func parseInterval(v json.JSON, out *int64) error {
	if v.Type() != json.StringJSONType {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"option %q must be a string, got %s", "interval", v.Type())
	}
	sptr, err := v.AsText()
	if err != nil || sptr == nil {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"option %q must be a string", "interval")
	}
	d, err := duration.ParseInterval(duration.IntervalStyle_POSTGRES, *sptr, types.DefaultIntervalTypeMetadata)
	if err != nil {
		return pgerror.Wrapf(err, pgcode.InvalidParameterValue, "invalid interval %q", *sptr)
	}
	if d.Months != 0 {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"interval must not contain month or year components")
	}
	// Days and sub-day Nanos can each be near the int64 limits; a naive
	// d.Days*nanosPerDay + d.Nanos() could wrap silently past the
	// "must be positive" guard below. Check each step.
	const nanosPerDay = int64(24 * 60 * 60 * 1_000_000_000)
	daysAsNanos, ok := arith.MulHalfPositiveWithOverflow(d.Days, nanosPerDay)
	if !ok {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"interval is out of range")
	}
	nanos, ok := arith.AddWithOverflow(daysAsNanos, d.Nanos())
	if !ok {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"interval is out of range")
	}
	if nanos <= 0 {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"interval must be positive")
	}
	const tenSecNanos = int64(10 * 1_000_000_000)
	if nanos%tenSecNanos != 0 {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"interval must be a positive multiple of 10s; got %s", *sptr)
	}
	*out = nanos
	return nil
}

// parseSources parses a JSON array of strings. Empty array yields
// nil (no source filter). Duplicates are dropped (first wins): the
// per-source dispatch path fans out one query per element, so leaving
// duplicates in would emit the same datapoint multiple times.
func parseSources(v json.JSON, out *[]string) error {
	if v.Type() != json.ArrayJSONType {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"option %q must be an array of strings, got %s", "sources", v.Type())
	}
	n := v.Len()
	if n == 0 {
		*out = nil
		return nil
	}
	result := make([]string, 0, n)
	seen := make(map[string]struct{}, n)
	for i := range n {
		elem, err := v.FetchValIdx(i)
		if err != nil {
			return err
		}
		if elem.Type() != json.StringJSONType {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"option %q must contain only strings; element %d is %s",
				"sources", i, elem.Type())
		}
		s, err := elem.AsText()
		if err != nil || s == nil {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"option %q element %d is not a valid string", "sources", i)
		}
		if _, ok := seen[*s]; ok {
			continue
		}
		seen[*s] = struct{}{}
		result = append(result, *s)
	}
	*out = result
	return nil
}
