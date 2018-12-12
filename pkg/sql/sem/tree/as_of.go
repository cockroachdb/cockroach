// Copyright 2018 The Cockroach Authors.
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

package tree

import (
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

// EvalAsOfTimestamp evaluates the timestamp argument to an AS OF SYSTEM TIME query.
func EvalAsOfTimestamp(
	asOf AsOfClause, max hlc.Timestamp, semaCtx *SemaContext, evalCtx *EvalContext,
) (hlc.Timestamp, error) {
	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	scalarProps := &semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	scalarProps.Require("AS OF SYSTEM TIME", RejectSpecial|RejectSubqueries)

	te, err := asOf.Expr.TypeCheck(semaCtx, types.String)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	if !IsConst(evalCtx, te) {
		return hlc.Timestamp{}, errors.Errorf("AS OF SYSTEM TIME: only constant expressions are allowed")
	}
	d, err := te.Eval(evalCtx)
	if err != nil {
		return hlc.Timestamp{}, err
	}

	var ts hlc.Timestamp
	var convErr error

	switch d := d.(type) {
	case *DString:
		s := string(*d)
		// Allow nanosecond precision because the timestamp is only used by the
		// system and won't be returned to the user over pgwire.
		if dt, err := ParseDTimestamp(evalCtx, s, time.Nanosecond); err == nil {
			ts.WallTime = dt.Time.UnixNano()
			break
		}
		// Attempt to parse as a decimal.
		if dec, _, err := apd.NewFromString(s); err == nil {
			ts, convErr = DecimalToHLC(dec)
			break
		}
		// Attempt to parse as an interval.
		if iv, err := ParseDInterval(s); err == nil {
			ts.WallTime = duration.Add(evalCtx, evalCtx.GetStmtTimestamp(), iv.Duration).UnixNano()
			break
		}
		convErr = errors.Errorf("AS OF SYSTEM TIME: value is neither timestamp, decimal, nor interval")
	case *DInt:
		ts.WallTime = int64(*d)
	case *DDecimal:
		ts, convErr = DecimalToHLC(&d.Decimal)
	case *DInterval:
		ts.WallTime = duration.Add(evalCtx, evalCtx.GetStmtTimestamp(), d.Duration).UnixNano()
	default:
		convErr = errors.Errorf("AS OF SYSTEM TIME: expected timestamp, decimal, or interval, got %s (%T)", d.ResolvedType(), d)
	}
	if convErr != nil {
		return ts, convErr
	}

	var zero hlc.Timestamp
	if ts == zero {
		return ts, errors.Errorf("AS OF SYSTEM TIME: zero timestamp is invalid")
	} else if ts.Less(zero) {
		return ts, errors.Errorf("AS OF SYSTEM TIME: timestamp before 1970-01-01T00:00:00Z is invalid")
	} else if max.Less(ts) {
		return ts, errors.Errorf("AS OF SYSTEM TIME: cannot specify timestamp in the future")
	}
	return ts, nil
}

// DecimalToHLC performs the conversion from an inputted DECIMAL datum for an
// AS OF SYSTEM TIME query to an HLC timestamp.
func DecimalToHLC(d *apd.Decimal) (hlc.Timestamp, error) {
	// Format the decimal into a string and split on `.` to extract the nanosecond
	// walltime and logical tick parts.
	// TODO(mjibson): use d.Modf() instead of converting to a string.
	s := d.Text('f')
	parts := strings.SplitN(s, ".", 2)
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return hlc.Timestamp{}, errors.Wrap(err, "AS OF SYSTEM TIME: parsing argument")
	}
	var logical int64
	if len(parts) > 1 {
		// logicalLength is the number of decimal digits expected in the
		// logical part to the right of the decimal. See the implementation of
		// cluster_logical_timestamp().
		const logicalLength = 10
		p := parts[1]
		if lp := len(p); lp > logicalLength {
			return hlc.Timestamp{}, errors.Errorf("AS OF SYSTEM TIME: logical part has too many digits")
		} else if lp < logicalLength {
			p += strings.Repeat("0", logicalLength-lp)
		}
		logical, err = strconv.ParseInt(p, 10, 32)
		if err != nil {
			return hlc.Timestamp{}, errors.Wrap(err, "AS OF SYSTEM TIME: parsing argument")
		}
	}
	return hlc.Timestamp{
		WallTime: nanos,
		Logical:  int32(logical),
	}, nil
}
