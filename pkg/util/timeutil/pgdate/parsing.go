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

package pgdate

import (
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// These are sentinel values for handling special values:
// https://www.postgresql.org/docs/10/static/datatype-datetime.html#DATATYPE-DATETIME-SPECIAL-TABLE
var (
	TimeEpoch            = timeutil.Unix(0, 0)
	TimeInfinity         = timeutil.Unix(math.MaxInt64, math.MaxInt64)
	TimeNegativeInfinity = timeutil.Unix(math.MinInt64, math.MinInt64)
)

//go:generate stringer -type=ParseMode

// ParseMode controls the resolution of ambiguous date formats such as
// `01/02/03`.
type ParseMode uint

const (
	// ParseModeYMD is the default
	ParseModeYMD ParseMode = iota
	ParseModeDMY
	ParseModeMDY
)

// ParseDate converts a string into a time value.
func ParseDate(now time.Time, mode ParseMode, s string) (time.Time, error) {
	fe := fieldExtract{
		now:      now,
		mode:     mode,
		required: dateRequiredFields,
		// We allow time fields to be provided, but we'll ignore them.
		wanted: dateTimeFields,
	}

	if t, err := extract(fe, s); err == nil {
		return t, nil
	} else {
		return TimeEpoch, parseError(err, "date", s)
	}
}

// ParseTime converts a string into a time value on the epoch day.
func ParseTime(now time.Time, s string) (time.Time, error) {
	fe := fieldExtract{
		now:      now,
		required: timeRequiredFields,
		wanted:   timeFields,
	}

	if t, err := extract(fe, s); err == nil {
		return t, nil
	} else {
		return TimeEpoch, parseError(err, "time", s)
	}
}

// ParseTime converts a string into a timestamp.
func ParseTimestamp(now time.Time, mode ParseMode, s string) (time.Time, error) {
	fe := fieldExtract{
		mode:     mode,
		now:      now,
		required: dateTimeRequiredFields,
		wanted:   dateTimeFields,
	}

	if t, err := extract(fe, s); err == nil {
		return t, nil
	} else {
		return TimeEpoch, parseError(err, "timestamp", s)
	}
}

func parseError(err error, kind string, s string) error {
	if _, ok := err.(*pgerror.Error); ok {
		return err
	}
	return errors.Errorf("could not parse \"%s\" as type %s", s, kind)
}
