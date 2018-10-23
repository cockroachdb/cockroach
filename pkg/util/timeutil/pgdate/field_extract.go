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
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/pkg/errors"
)

// numberChunk associates a value with a leading prefix,
// such as '-' or '/'.
type numberChunk struct {
	prefix rune
	// The numeric value of the chunk.
	v int
	// The magnitude of the value (i.e. how many digits).
	magnitude int
}

func (n numberChunk) String() string {
	if n.prefix == utf8.RuneError {
		return fmt.Sprintf("%d", n.v)
	}
	return fmt.Sprintf("%v%d", n.prefix, n.v)
}

// fieldExtract manages the state of a date/time parsing operation.
// This handles cases where a field, such as Julian date,
// would conflict with also setting the year.
type fieldExtract struct {
	// The field data is stored in a fixed-size array.
	data [fieldMaximum + 1]int
	// Tracks the fields that have been set, to distinguish 0 from unset.
	has fieldSet
	// Provides a time for evaluating relative dates as well as a
	// timezone.
	now  time.Time
	mode ParseMode
	// This indicates that the value in the year field was only
	// two digits and should be adjusted to make it recent.
	tweakYear bool
	// Tracks the sign of the timezone offset.  We need to track
	// this separately from the sign of the tz1 value in case
	// we're trying to store a (nonsensical) value like -0030.
	tzSign int
	// The fields that must be present to succeed.
	required fieldSet
	// Tracks the fields that we want to extract.
	wanted fieldSet
}

var (
	dateFields         = newFieldSet(fieldYear, fieldMonth, fieldDay, fieldEra)
	dateRequiredFields = newFieldSet(fieldYear, fieldMonth, fieldDay)

	timeFields = newFieldSet(
		fieldHour, fieldMinute, fieldSecond, fieldFraction, fieldMeridian, fieldTZ1, fieldTZ2)
	timeRequiredFields = newFieldSet(fieldHour, fieldMinute)

	dateTimeFields         = dateFields.AddAll(timeFields)
	dateTimeRequiredFields = dateRequiredFields.AddAll(timeRequiredFields)
)

// Get returns the value of the requested field and whether or not
// that field has indeed been set.
func (fe *fieldExtract) Get(field field) (int, bool) {
	return fe.data[field], fe.has.Has(field)
}

func (fe *fieldExtract) interpretNumber(chunk numberChunk, textMonth bool) error {
	switch {
	case chunk.prefix == '.':
		// It's either a yyyy.ddd or we're looking at fractions.
		switch {
		case chunk.magnitude == 3 && !fe.Wants(fieldYear) && fe.Wants(fieldMonth) && fe.Wants(fieldDay):
			return fe.SetDayOfYear(chunk.v)

		case !fe.Wants(fieldSecond) && fe.Wants(fieldFraction):
			// We want to adjust the fraction into a nanosecond value.
			// We should accept at most 9 digits.
			var mult int
			switch chunk.magnitude {
			case 1:
				mult = 100000000
			case 2:
				mult = 10000000
			case 3:
				mult = 1000000
			case 4:
				mult = 100000
			case 5:
				mult = 10000
			case 6:
				mult = 1000
			case 7:
				mult = 100
			case 8:
				mult = 10
			case 9:
				mult = 1
			default:
				return errors.New("fractional part too long")
			}
			return fe.Set(fieldFraction, chunk.v*mult)

		default:
			return errors.Errorf("cannot interpret %s", chunk)
		}

	case chunk.magnitude == 3 && !fe.Wants(fieldYear) && chunk.v >= 1 && chunk.v <= 366:
		// A three-digit value could be a day-of-year.
		return fe.SetDayOfYear(chunk.v)

	case fe.Wants(fieldYear) && fe.Wants(fieldMonth) && fe.Wants(fieldDay) && chunk.prefix != ':':
		// We're starting from scratch
		switch {
		case chunk.magnitude >= 6:
			// We're looking at yymmdd or yyyymmdd
			v := chunk.v
			// Record whether or not it's a two-digit year.
			fe.tweakYear = chunk.magnitude == 6
			if err := fe.Set(fieldDay, v%100); err != nil {
				return err
			}
			v /= 100
			if err := fe.Set(fieldMonth, v%100); err != nil {
				return err
			}
			v /= 100
			return fe.Set(fieldYear, v)

		case chunk.magnitude >= 3 || fe.mode == ParseModeYMD:
			// A three- or four-digit number must be a year.  If we are in a
			// year-first mode, we'll accept the first chunk and possibly
			// adjust a two-digit value later on.  This means that
			// 99 would get adjusted to 1999, but 0099 would not.
			if chunk.magnitude <= 2 {
				fe.tweakYear = true
			}
			return fe.Set(fieldYear, chunk.v)
		case fe.mode == ParseModeDMY:
			return fe.Set(fieldDay, chunk.v)
		default:
			return fe.Set(fieldMonth, chunk.v)
		}

	case !fe.Wants(fieldYear) && fe.Wants(fieldMonth) && fe.Wants(fieldDay) && chunk.prefix != ':':
		// Must be a year-month-day
		return fe.Set(fieldMonth, chunk.v)

	case fe.Wants(fieldYear) && !fe.Wants(fieldMonth) && fe.Wants(fieldDay) && chunk.prefix != ':':
		// The month has been set; this could be mm-dd-yy or a
		// format with a textual month entry.  If we know that the
		// month was set in the first phase, we'll look for an obvious year
		// or defer to the parsing mode.
		if textMonth && (chunk.magnitude >= 3 || fe.mode == ParseModeYMD) {
			if chunk.magnitude <= 2 {
				fe.tweakYear = true
			}
			return fe.Set(fieldYear, chunk.v)
		}
		return fe.Set(fieldDay, chunk.v)

	case !fe.Wants(fieldYear) && !fe.Wants(fieldMonth) && fe.Wants(fieldDay) && chunk.prefix != ':':
		// We should be looking at just the day component.  However, we
		// need to handle the case where we saw a two-digit year, but
		// we're now looking at a value that can only be a year.
		// If this happens, we'll swap the year and day, then set the year.
		if textMonth && chunk.magnitude >= 3 && fe.tweakYear {
			fe.tweakYear = false
			year, _ := fe.Get(fieldYear)
			if err := fe.Set(fieldDay, year); err != nil {
				return err
			}
			return fe.Reset(fieldYear, chunk.v)
		}
		return fe.Set(fieldDay, chunk.v)

	case fe.Wants(fieldYear) && fe.Wants(fieldMonth) && !fe.Wants(fieldDay) && chunk.prefix != ':':
		// Must be looking at a dd-mm-yy format.
		return fe.Set(fieldMonth, chunk.v)

	case fe.Wants(fieldYear) && !fe.Wants(fieldMonth) && !fe.Wants(fieldDay) && chunk.prefix != ':':
		// Have month and day, last field must be a year.
		if chunk.magnitude <= 2 {
			fe.tweakYear = true
		}
		return fe.Set(fieldYear, chunk.v)

	case fe.Wants(fieldTZ1) && (chunk.prefix == '-' || chunk.prefix == '+'):
		// We're looking at a numeric timezone specifier.  This can be
		// from one to four digits (or we might see another chunk if 8:30).

		if chunk.prefix == '-' {
			fe.tzSign = -1
		} else {
			fe.tzSign = 1
		}

		switch chunk.magnitude {
		case 1, 2:
			// +8 +08
			return fe.Set(fieldTZ1, chunk.v)
		case 3, 4:
			// +830 +0830
			if err := fe.Set(fieldTZ2, chunk.v%100); err != nil {
				return err
			}
			return fe.Set(fieldTZ1, chunk.v/100)
		default:
			return errors.Errorf("invalid timezone offset: %v", chunk)
		}

	case !fe.Wants(fieldTZ1) && fe.Wants(fieldTZ2):
		if chunk.prefix == ':' {
			// We're looking at the second half of a timezone like +8:30.
			// This would be the final match in any well-formatted input.
			return fe.Set(fieldTZ2, chunk.v)
		} else if err := fe.Set(fieldTZ2, 0); err != nil {
			return err
		}
		// Otherwise, we need to re-process this chunk since this case
		// will always match immediately after TZ1 is set.
		return fe.interpretNumber(chunk, textMonth)

	case fe.Wants(fieldHour) && fe.Wants(fieldMinute) && fe.Wants(fieldSecond) && chunk.prefix != ':':
		v := chunk.v
		seconds := 0
		switch chunk.magnitude {
		case 1, 2:
			// h or hh
			return fe.Set(fieldHour, v)
		case 6:
			// hhmmss
			seconds = v % 100
			v /= 100
			fallthrough
		case 4:
			// hhmm, but either force seconds to 0 or use value from above
			if err := fe.Set(fieldSecond, seconds); err != nil {
				return err
			}
			if err := fe.Set(fieldMinute, v%100); err != nil {
				return err
			}
			return fe.Set(fieldHour, v/100)

		default:
			return errors.Errorf("unexpected number of digits for hour in %v", chunk)

		}

	case fe.Wants(fieldMinute) && chunk.prefix == ':':
		return fe.Set(fieldMinute, chunk.v)

	case fe.Wants(fieldSecond) && chunk.prefix == ':':
		return fe.Set(fieldSecond, chunk.v)

	default:
		return errors.Errorf("could not interpret %v", chunk)
	}
}

// Force sets the field without performing any sanity checks.
// This should be used sparingly.
func (fe *fieldExtract) Force(field field, v int) {
	fe.data[field] = v
	fe.has = fe.has.Add(field)
	fe.wanted = fe.wanted.Clear(field)
}

// Reset replaces a value of an already-set field.
func (fe *fieldExtract) Reset(field field, v int) error {
	if !fe.has.Has(field) {
		return errors.Errorf("field %s is not already set", field.String()[5:])
	}
	fe.data[field] = v
	return nil
}

// Set updates the value of the given field, returning an error if
// the field has already been set or if the value is out-of-range.
// This method also removes redundant fields from the wanted set.
func (fe *fieldExtract) Set(field field, v int) error {
	if !fe.wanted.Has(field) {
		return errors.Errorf("field %s is not wanted in %v", field.String()[5:], fe.wanted)
	}
	fe.data[field] = v
	fe.has = fe.has.Add(field)
	fe.wanted = fe.wanted.Clear(field)

	return nil
}

// SetDayOfYear updates the month and day fields to reflect the
// given day-of-year.  The year must have been previously set.
func (fe *fieldExtract) SetDayOfYear(doy int) error {
	y, ok := fe.Get(fieldYear)
	if !ok {
		return errors.New("year must be set before day of year")
	}
	y, m, d := julianDayToDate(dateToJulianDay(y, 1, 1) + doy - 1)
	if err := fe.Reset(fieldYear, y); err != nil {
		return err
	}
	if err := fe.Set(fieldMonth, m); err != nil {
		return err
	}
	return fe.Set(fieldDay, d)
}

func (fe *fieldExtract) String() string {
	ret := "[ "
	for f := fieldMinimum; f <= fieldMaximum; f++ {
		if v, ok := fe.Get(f); ok {
			ret += fmt.Sprintf("%s: %d ", f.String()[5:], v)
		}
	}
	ret += "]"
	return ret
}

// Validate ensures that the data in the extract is reasonable.
func (fe *fieldExtract) Validate() error {
	// If we have any required fields, we must have all required fields.
	if fe.has.HasAny(dateRequiredFields) && !fe.has.HasAll(dateRequiredFields) {
		return errors.Errorf("have some but not all date fields")
	}
	if fe.has.HasAny(timeRequiredFields) && !fe.has.HasAll(timeRequiredFields) {
		return errors.Errorf("have some but not all time fields")
	}

	if year, ok := fe.Get(fieldYear); ok {
		// Update for BC dates.
		if era, ok := fe.Get(fieldEra); ok && era < 0 {
			// No year 0
			if year <= 0 {
				return errors.New("no year 0 in AD/BC notation")
			}
			// Normalize to a negative year
			if err := fe.Reset(fieldYear, 1-year); err != nil {
				return err
			}
		} else if fe.tweakYear {
			if year < 0 {
				return errors.New("negative year not allowed")
			}
			if year < 70 {
				year += 2000
			} else if year < 100 {
				year += 1900
			}
			if err := fe.Reset(fieldYear, year); err != nil {
				return err
			}
		}

		if month, ok := fe.Get(fieldMonth); ok {
			if month < 1 || month > 12 {
				return errors.New("month out of range")
			}

			if day, ok := fe.Get(fieldDay); ok {
				var maxDay int
				if isLeap(year) {
					maxDay = daysInMonth[1][month]
				} else {
					maxDay = daysInMonth[0][month]
				}
				if day < 1 || day > maxDay {
					return errors.New("day out of range")
				}
			}
		}
	}

	if hour, ok := fe.Get(fieldHour); ok {
		hasDate := fe.has.HasAny(dateRequiredFields)

		meridian, _ := fe.Get(fieldMeridian)
		switch meridian {
		case fieldValueAM:
			switch {
			case hour < 0 || hour > 12:
				return errors.New("hour out of range")
			case hour == 12:
				if err := fe.Reset(fieldHour, 0); err != nil {
					return err
				}
			}

		case fieldValuePM:
			switch {
			case hour < 0 || hour > 12:
				return errors.New("hour out of range")
			case hour == 12:
				// 12 PM -> 12
			default:
				// 1 PM -> 13
				if err := fe.Reset(fieldHour, hour+12); err != nil {
					return err
				}
			}

		default:
			// 24:00:00 is the maximum-allowed value
			if hour < 0 || (hasDate && hour > 24) || (!hasDate && hour > 23) {
				return errors.New("hour out of range")
			}
		}

		minute, _ := fe.Get(fieldMinute)
		if minute < 0 || minute > 59 {
			return errors.New("minute out of range")
		}

		second, _ := fe.Get(fieldSecond)
		if second < 0 || (hasDate && second > 60) || (!hasDate && second > 59) {
			return errors.New("second out of range")
		}

		nanos, _ := fe.Get(fieldFraction)
		if nanos < 0 {
			return errors.New("nanos out of range")
		}

		x := time.Duration(hour)*time.Hour +
			time.Duration(minute)*time.Minute +
			time.Duration(second)*time.Second +
			time.Duration(nanos)*time.Nanosecond
		if x > 24*time.Hour {
			return errors.Errorf("time out of range: %d", x)
		}

	}

	return nil
}

// Wants returns whether or not the field is wanted in the extract.
func (fe *fieldExtract) Wants(field field) bool {
	return fe.wanted.Has(field)
}

// WantsAll returns whether or not all fields are wanted in the extract.
func (fe *fieldExtract) WantsAll(fields fieldSet) bool {
	return fe.wanted.HasAll(fields)
}

// Zero sets multiple fields to zero.
func (fe *fieldExtract) Zero(fields fieldSet) error {
	for _, field := range fields.AsSlice() {
		if err := fe.Set(field, 0); err != nil {
			return err
		}
	}
	return nil
}
