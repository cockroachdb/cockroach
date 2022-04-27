// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// DecimalToHLC performs the conversion from an inputted DECIMAL datum for an
// AS OF SYSTEM TIME query to an HLC timestamp.
func DecimalToHLC(d *apd.Decimal) (hlc.Timestamp, error) {
	if d.Negative {
		return hlc.Timestamp{}, pgerror.Newf(pgcode.Syntax, "cannot be negative")
	}
	var integral, fractional apd.Decimal
	d.Modf(&integral, &fractional)
	timestamp, err := integral.Int64()
	if err != nil {
		return hlc.Timestamp{}, pgerror.Wrapf(err, pgcode.Syntax, "converting timestamp to integer") // should never happen
	}
	if fractional.IsZero() {
		// there is no logical portion to this clock
		return hlc.Timestamp{WallTime: timestamp}, nil
	}

	var logical apd.Decimal
	multiplier := apd.New(1, 10)
	condition, err := apd.BaseContext.Mul(&logical, &fractional, multiplier)
	if err != nil {
		return hlc.Timestamp{}, pgerror.Wrapf(err, pgcode.Syntax, "determining value of logical clock")
	}
	if _, err := condition.GoError(apd.DefaultTraps); err != nil {
		return hlc.Timestamp{}, pgerror.Wrapf(err, pgcode.Syntax, "determining value of logical clock")
	}

	counter, err := logical.Int64()
	if err != nil {
		return hlc.Timestamp{}, pgerror.Newf(pgcode.Syntax, "logical part has too many digits")
	}
	if counter > 1<<31 {
		return hlc.Timestamp{}, pgerror.Newf(pgcode.Syntax, "logical clock too large: %d", counter)
	}
	return hlc.Timestamp{
		WallTime: timestamp,
		Logical:  int32(counter),
	}, nil
}

// ParseHLC parses a string representation of an `hlc.Timestamp`.
// This differs from hlc.ParseTimestamp in that it parses the decimal
// serialization of an hlc timestamp as opposed to the string serialization
// performed by hlc.Timestamp.String().
//
// This function is used to parse:
//
//   1580361670629466905.0000000001
//
// hlc.ParseTimestamp() would be used to parse:
//
//   1580361670.629466905,1
//
func ParseHLC(s string) (hlc.Timestamp, error) {
	dec, _, err := apd.NewFromString(s)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	return DecimalToHLC(dec)
}
