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
	"strconv"
	"strings"
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
	// Stores a reference to one of the sentinel values, to be returned
	// by the makeDateTime() functions
	sentinel *time.Time
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

// Extract is the top-level function.  It attempts to break the input
// string into a collection of date/time fields in order to populate a
// fieldExtract.
func (fe *fieldExtract) Extract(s string) error {
	// Break the string into alphanumeric chunks.
	textChunks, _ := chunk(s)

	// Create a place to store extracted numeric info.
	numbers := make([]numberChunk, 0, len(textChunks))
	appendNumber := func(prefix, number string) error {
		v, err := strconv.Atoi(number)
		if err != nil {
			return err
		}
		lastRune, _ := utf8.DecodeLastRuneInString(prefix)
		numbers = append(numbers, numberChunk{prefix: lastRune, v: v, magnitude: len(number)})
		return nil
	}

	// Certain keywords should result in some kind of sentinel value,
	// but we want to ensure that we accept only a single sentinel chunk.
	matchedSentinel := func(value *time.Time, match string) error {
		if fe.sentinel != nil {
			return errors.Errorf("unexpected input: %s", match)
		}
		fe.sentinel = value
		return nil
	}

	var leftoverText string

	// First, we'll try to pluck out any keywords that exist in the input.
	// If a chunk is not a keyword or other special-case pattern, it
	// must be a numeric value, which we'll pluck out for a second
	// pass. If we see certain sentinel values, we'll pick them out,
	// but keep going to ensure that the user hasn't written something
	// like "epoch infinity".
	for _, chunk := range textChunks {
		match := strings.ToLower(chunk.Match)

		switch match {
		case keywordEpoch:
			if err := matchedSentinel(&TimeEpoch, match); err != nil {
				return err
			}

		case keywordInfinity:
			if strings.HasSuffix(chunk.NotMatch, "-") {
				if err := matchedSentinel(&TimeNegativeInfinity, match); err != nil {
					return err
				}
			} else {
				if err := matchedSentinel(&TimeInfinity, match); err != nil {
					return err
				}
			}

		case keywordNow:
			if err := matchedSentinel(&fe.now, match); err != nil {
				return err
			}

		default:
			// Fan out to other keyword-based extracts.
			if m, ok := keywordSetters[match]; ok {
				if err := m(fe, match); err != nil {
					return err
				}
				continue
			}

			// At this point, the most probable case is that we have a
			// numeric input.
			if err := appendNumber(chunk.NotMatch, match); err == nil {
				continue
			}

			// Handle the oddball Z and Zulu suffixes. Try stripping the
			// suffix and appending the resulting number.
			if strings.HasSuffix(match, keywordZ) {
				if err := fieldSetterUTC(fe, ""); err != nil {
					return err
				}
				maybeMatch := match[:len(match)-len(keywordZ)]
				if err := appendNumber(chunk.NotMatch, maybeMatch); err == nil {
					continue
				}

			} else if strings.HasSuffix(match, keywordZulu) {
				if err := fieldSetterUTC(fe, ""); err != nil {
					return err
				}
				maybeMatch := match[:len(match)-len(keywordZulu)]
				if err := appendNumber(chunk.NotMatch, maybeMatch); err == nil {
					continue
				}
			}

			// Try to parse Julian dates.
			if matched, err := fieldSetterJulianDate(fe, match); matched {
				if err != nil {
					return err
				}
				continue
			}

			// Save off any leftover text, it might be a timezone name.
			leftoverText += strings.TrimSpace(chunk.NotMatch) + chunk.Match
		}
	}

	// See if our leftover text is a timezone name.
	if leftoverText != "" {
		if loc, err := zoneCacheInstance.LoadLocation(leftoverText); err == nil {
			// Save off the timezone for later resolution to an offset.
			fe.now = fe.now.In(loc)

			// Since we're using a named location, we must have a date
			// in order to compute daylight-savings time.
			fe.required = fe.required.AddAll(dateRequiredFields)

			// Remove TZ fields from the wanted list, but add a date
			// in order to resolve the location's DST.  Also, if we had a
			// text month, ensure that it's also not in the wanted field.
			fe.wanted = fe.wanted.AddAll(dateFields).ClearAll(fe.has.Add(fieldTZHour).Add(fieldTZMinute).Add(fieldTZSecond))
		} else {
			return errors.Errorf("could not interpret leftovers: %s", leftoverText)
		}
	}

	if fe.sentinel != nil {
		return nil
	}

	// In the second pass, we'll use pattern-matching and the knowledge
	// of which fields have already been set in order to keep picking
	// out field data.
	textMonth := !fe.Wants(fieldMonth)
	for _, n := range numbers {
		if fe.wanted == 0 {
			return errors.Errorf("too many input chunks")
		}
		if err := fe.InterpretNumber(n, textMonth); err != nil {
			return err
		}
	}

	return fe.Validate()
}

// Get returns the value of the requested field and whether or not
// that field has indeed been set.
func (fe *fieldExtract) Get(field field) (int, bool) {
	return fe.data[field], fe.has.Has(field)
}

func (fe *fieldExtract) InterpretNumber(chunk numberChunk, textMonth bool) error {
	switch {
	case chunk.prefix == '.':
		// It's either a yyyy.ddd or we're looking at fractions.
		switch {
		case chunk.magnitude == 3 && !fe.Wants(fieldYear) && fe.Wants(fieldMonth) && fe.Wants(fieldDay):
			// Looks like a yyyy.ddd
			return fe.SetDayOfYear(chunk.v)

		case !fe.Wants(fieldSecond) && fe.Wants(fieldNanos):
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
			return fe.Set(fieldNanos, chunk.v*mult)

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

	case fe.Wants(fieldTZHour) && (chunk.prefix == '-' || chunk.prefix == '+'):
		// We're looking at a numeric timezone specifier.  This can be
		// from one to six digits (or we might see another chunk if 08:30:00).

		if chunk.prefix == '-' {
			fe.tzSign = -1
		} else {
			fe.tzSign = 1
		}

		v := chunk.v
		seconds := 0
		switch chunk.magnitude {
		case 1, 2:
			// h or hh
			return fe.Set(fieldTZHour, v)
		case 6:
			// hhmmss
			seconds = v % 100
			v /= 100
			fallthrough
		case 4:
			// hhmm, but either force seconds to 0 or use value from above
			if err := fe.Set(fieldTZSecond, seconds); err != nil {
				return err
			}
			if err := fe.Set(fieldTZMinute, v%100); err != nil {
				return err
			}
			return fe.Set(fieldTZHour, v/100)

		default:
			return errors.Errorf("unexpected number of digits for timezone in %v", chunk)
		}

	case !fe.Wants(fieldTZHour) && fe.Wants(fieldTZMinute):
		if chunk.prefix == ':' {
			// We're looking at the second half of a timezone like +8:30.
			// This would be the final match in any well-formatted input.
			return fe.Set(fieldTZMinute, chunk.v)
		}

		// We no longer except to see any timezone data, so we'll
		// mark is as completed and re-interpret the chunk.
		fe.has = fe.has.AddAll(tzFields)
		fe.wanted = fe.wanted.ClearAll(tzFields)
		return fe.InterpretNumber(chunk, textMonth)

	case !fe.Wants(fieldTZHour) && !fe.Wants(fieldTZMinute) && fe.Wants(fieldTZSecond):
		if chunk.prefix == ':' {
			// We're looking at the third part of a timezone like +8:30:15.
			// This would be the final match in any well-formatted input.
			return fe.Set(fieldTZSecond, chunk.v)
		}

		// We no longer except to see any timezone data, so we'll
		// mark is as completed and re-interpret the chunk.
		fe.has = fe.has.Add(fieldTZSecond)
		fe.wanted = fe.wanted.Clear(fieldTZSecond)
		return fe.InterpretNumber(chunk, textMonth)

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
			return errors.Errorf("unexpected number of digits for time in %v", chunk)
		}

	case fe.Wants(fieldMinute) && chunk.prefix == ':':
		return fe.Set(fieldMinute, chunk.v)

	case fe.Wants(fieldSecond) && chunk.prefix == ':':
		return fe.Set(fieldSecond, chunk.v)
	}
	return errors.Errorf("could not interpret %v", chunk)
}

// Force sets the field without performing any sanity checks.
// This should be used sparingly.
func (fe *fieldExtract) Force(field field, v int) {
	fe.data[field] = v
	fe.has = fe.has.Add(field)
	fe.wanted = fe.wanted.Clear(field)
}

// MakeDate returns a time.Time containing only the date components
// of the extract.
func (fe *fieldExtract) MakeDate() time.Time {
	if fe.sentinel != nil {
		return *fe.sentinel
	}

	year, _ := fe.Get(fieldYear)
	month, _ := fe.Get(fieldMonth)
	day, _ := fe.Get(fieldDay)
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, fe.MakeLocation())
}

// MakeTime returns only the time component of the extract.
// If the user provided a named timezone, as opposed
// to a fixed offset, we will resolve the named zone to an offset
// based on the best-available date information.
func (fe *fieldExtract) MakeTime() time.Time {
	if fe.sentinel != nil {
		return *fe.sentinel
	}

	ret := fe.MakeTimestamp()
	hour, min, sec := ret.Clock()
	_, offset := ret.Zone()
	return time.Date(0, 1, 1, hour, min, sec, ret.Nanosecond(), time.FixedZone("", offset))
}

// MakeTimestamp returns a time.Time containing all extracted information.
func (fe *fieldExtract) MakeTimestamp() time.Time {
	if fe.sentinel != nil {
		return *fe.sentinel
	}

	year, _ := fe.Get(fieldYear)
	month, _ := fe.Get(fieldMonth)
	day, _ := fe.Get(fieldDay)
	hour, _ := fe.Get(fieldHour)
	min, _ := fe.Get(fieldMinute)
	sec, _ := fe.Get(fieldSecond)
	nano, _ := fe.Get(fieldNanos)

	return time.Date(year, time.Month(month), day, hour, min, sec, nano, fe.MakeLocation())
}

// MakeLocation returns the timezone information stored in the extract,
// or returns the default location.
func (fe *fieldExtract) MakeLocation() *time.Location {
	tzHour, ok := fe.Get(fieldTZHour)
	if !ok {
		return fe.now.Location()
	}
	tzMin, _ := fe.Get(fieldTZMinute)
	tzSec, _ := fe.Get(fieldTZSecond)

	tzHour *= fe.tzSign
	tzMin *= fe.tzSign
	tzSec *= fe.tzSign

	return zoneCacheInstance.FixedZone(tzHour, tzMin, tzSec)
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

// Validate ensures that the data in the extract is reasonable. It also
// performs some field fixups, such as converting two-digit years
// to actual values and adjusting for AM/PM.
func (fe *fieldExtract) Validate() error {
	// If we have any of the required fields, we must have all of the required fields.
	if fe.has.HasAny(dateRequiredFields) && !fe.has.HasAll(dateRequiredFields) {
		return errors.Errorf("have some but not all date fields")
	}
	if fe.has.HasAny(timeRequiredFields) && !fe.has.HasAll(timeRequiredFields) {
		return errors.Errorf("have some but not all time fields")
	}
	if !fe.has.HasAll(fe.required) {
		return errors.Errorf("missing required fields: %v vs %v", fe.required, fe.has)
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

		nanos, _ := fe.Get(fieldNanos)
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

// Zero sets multiple fields to zero.
func (fe *fieldExtract) Zero(fields fieldSet) error {
	for _, field := range fields.AsSlice() {
		if err := fe.Set(field, 0); err != nil {
			return err
		}
	}
	return nil
}
