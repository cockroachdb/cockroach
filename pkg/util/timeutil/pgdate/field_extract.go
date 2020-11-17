// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgdate

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
)

// numberChunk associates a value with a leading separator,
// such as '-' or '/'.
type numberChunk struct {
	separator rune
	// The numeric value of the chunk.
	v int
	// The magnitude of the value (i.e. how many digits).
	magnitude int
}

func (n numberChunk) String() string {
	if n.separator == utf8.RuneError {
		return fmt.Sprintf("%d", n.v)
	}
	return fmt.Sprintf("%v%d", n.separator, n.v)
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
	// The fields that must be present to succeed.
	required fieldSet
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
	// Tracks the fields that we want to extract.
	wanted fieldSet
}

// Extract is the top-level function.  It attempts to break the input
// string into a collection of date/time fields in order to populate a
// fieldExtract.
func (fe *fieldExtract) Extract(s string) error {
	// Break the string into alphanumeric chunks.
	textChunks := make([]stringChunk, fieldMaximum)
	count, _ := chunk(s, textChunks)

	if count < 0 {
		return inputErrorf("too many fields in input")
	} else if count == 0 {
		return inputErrorf("empty or blank input")
	}

	// Create a place to store extracted numeric info.
	numbers := make([]numberChunk, 0, fieldMaximum)

	appendNumber := func(prefix, number string) error {
		v, err := strconv.Atoi(number)
		if err != nil {
			return err
		}

		// Allow exactly one non-whitespace separator.
		s := ' '
		for _, r := range prefix {
			switch {
			case s == ' ':
				s = r
			case unicode.IsSpace(r):
			// Ignore whitespace characters.
			default:
				return inputErrorf(`detected multiple separators in "%s""`, prefix)
			}
		}

		numbers = append(numbers, numberChunk{separator: s, v: v, magnitude: len(number)})
		return nil
	}

	var leftoverText string

	// First, we'll try to pluck out any keywords that exist in the input.
	// If a chunk is not a keyword or other special-case pattern, it
	// must be a numeric value, which we'll pluck out for a second
	// pass. If we see certain sentinel values, we'll pick them out,
	// but keep going to ensure that the user hasn't written something
	// like "epoch infinity".
	for idx, chunk := range textChunks[:count] {
		match := strings.ToLower(chunk.Match)

		switch match {
		case keywordEpoch:
			if err := fe.matchedSentinel(TimeEpoch, match); err != nil {
				return err
			}

		case keywordInfinity:
			if strings.HasSuffix(chunk.NotMatch, "-") {
				if err := fe.matchedSentinel(TimeNegativeInfinity, match); err != nil {
					return err
				}
			} else {
				if err := fe.matchedSentinel(TimeInfinity, match); err != nil {
					return err
				}
			}

		case keywordNow:
			if err := fe.matchedSentinel(fe.now, match); err != nil {
				return err
			}

		default:
			// The most probable case is that we have a numeric input.
			if err := appendNumber(chunk.NotMatch, match); err == nil {
				continue
			}

			// Fan out to other keyword-based extracts.
			if m, ok := keywordSetters[match]; ok {
				if err := m(fe, match); err != nil {
					return err
				}

				// This detects a format like 01-02-Jan.  While we could
				// figure it out if one of those were a four-digit number,
				// this is consistent with PostgreSQL 10.5 behavior.
				// We should only ever see a text month in field 0 or 1.
				if idx == 2 && fe.Wants(fieldYear) && !fe.Wants(fieldMonth) && fe.Wants(fieldDay) {
					return inputErrorf("no such thing as a YDM or DYM format")
				}
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
			// We do this concatenation here because Region/City_Name
			// would get split into two chunks.
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
			return inputErrorf(`could not parse "%s"`, s)
		}
	}

	if fe.sentinel != nil {
		return nil
	}

	// In the second pass, we'll use pattern-matching and the knowledge
	// of which fields have already been set in order to keep picking
	// out field data.
	textMonth := !fe.Wants(fieldMonth)
	for i := range numbers {
		if fe.wanted == 0 {
			return inputErrorf("too many input fields")
		}
		if err := fe.interpretNumber(numbers, i, textMonth); err != nil {
			return err
		}
	}

	return fe.validate()
}

// Get returns the value of the requested field and whether or not
// that field has indeed been set.
func (fe *fieldExtract) Get(field field) (int, bool) {
	return fe.data[field], fe.has.Has(field)
}

// interpretNumber applies pattern-matching rules to figure out which
// field the next chunk of input should be applied to.
func (fe *fieldExtract) interpretNumber(numbers []numberChunk, idx int, textMonth bool) error {
	chunk := numbers[idx]
	switch {
	case chunk.separator == '.':
		// Example: 04:04:04.913231+00:00, a fractional second.
		//                   ^^^^^^
		// Example: 1999.123, a year + day-of-year.
		//               ^^^
		switch {
		case chunk.magnitude == 3 &&
			!fe.Wants(fieldYear) && fe.Wants(fieldMonth) && fe.Wants(fieldDay) &&
			chunk.v >= 1 && chunk.v <= 366:
			// Example: 1999 001, a year and day-of-year.
			//               ^^^
			// Example: 1999 123
			//               ^^^
			// BUT NOT: 1999 1
			return fe.SetDayOfYear(chunk)

		case !fe.Wants(fieldSecond) && fe.Wants(fieldNanos):
			// The only other place a period is valid is in a fractional
			// second.  We check to make sure that a second has been set.

			// We need to "right-pad" the parsed integer value to nine
			// places to wind up with a nanosecond value.  Values with
			// sub-nanosecond precision will be truncated.
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
				mult = 1
				for chunk.magnitude > 9 {
					chunk.magnitude--
					chunk.v = chunk.v / 10
				}
			}
			chunk.v *= mult
			return fe.SetChunk(fieldNanos, chunk)

		default:
			return inputErrorf("cannot interpret field: %s", chunk)
		}

	case chunk.magnitude == 3 &&
		!fe.Wants(fieldYear) && fe.Wants(fieldMonth) && fe.Wants(fieldDay) &&
		chunk.v >= 1 && chunk.v <= 366:
		// Example: 1999 001, a year and day-of-year.
		//               ^^^
		// Example: 1999 123
		//               ^^^
		// BUT NOT: 1999 1
		return fe.SetDayOfYear(chunk)

	case fe.Wants(fieldYear) && fe.Wants(fieldMonth) && fe.Wants(fieldDay):
		var nextSep rune
		if len(numbers) > idx+1 {
			nextSep = numbers[idx+1].separator
		}
		// Example: All date formats, we're starting from scratch.
		switch {
		// We examine the next separator to decide if this is a
		// concatenated date or a really long year. If it's a - or /
		// then this is one part of a date instead of the whole date.
		case chunk.magnitude >= 6 && chunk.separator != '-' && nextSep != '-' && nextSep != '/':
			// Example: "YYMMDD"
			//           ^^^^^^
			// Example: "YYYYMMDD"
			//           ^^^^^^^^
			// We're looking at some kind of concatenated date. We do want
			// to exclude large-magnitude, negative years from this test.

			// Record whether or not it's a two-digit year.
			fe.tweakYear = chunk.magnitude == 6
			if err := fe.Set(fieldDay, chunk.v%100); err != nil {
				return err
			}
			chunk.v /= 100
			if err := fe.Set(fieldMonth, chunk.v%100); err != nil {
				return err
			}
			chunk.v /= 100
			return fe.SetChunk(fieldYear, chunk)

		case chunk.magnitude >= 3 || fe.mode == ParseModeYMD:
			// Example: "YYYY MM DD"
			//           ^^^^
			// Example: "YYY MM DD"
			//           ^^^
			// Example: "YY MM DD"
			//           ^^
			// A three- or four-digit number must be a year.  If we are in a
			// year-first mode, we'll accept the first chunk and possibly
			// adjust a two-digit value later on.  This means that
			// 99 would get adjusted to 1999, but 0099 would not.
			if chunk.separator == '-' {
				chunk.v *= -1
			} else if chunk.magnitude <= 2 {
				fe.tweakYear = true
			}
			return fe.SetChunk(fieldYear, chunk)
		case fe.mode == ParseModeDMY:
			// Example: "DD MM YY"
			//           ^^
			// The first value is ambiguous, so we rely on the mode.
			return fe.SetChunk(fieldDay, chunk)
		case fe.mode == ParseModeMDY:
			// Example: "MM DD YY"
			//           ^^
			// The first value is ambiguous, so we rely on the mode.
			return fe.SetChunk(fieldMonth, chunk)
		}

	case !fe.Wants(fieldYear) && fe.Wants(fieldMonth) && fe.Wants(fieldDay):
		// Example: "YY MM DD"
		//              ^^
		// Example: "YYYY MM DD"
		//                ^^
		// We have the year set and are looking for the month and day.
		return fe.Set(fieldMonth, chunk.v)

	case fe.Wants(fieldYear) && !fe.Wants(fieldMonth) && fe.Wants(fieldDay):
		// Example: "YYYY Month DD"
		//           ^^^^
		// Example: "YYY Month DD"
		//           ^^^
		// Example: "MM DD YY"; only in MDY mode.
		//              ^^
		// Example: "Month DD YY"; only in MDY mode
		//                 ^^
		// Example: "DD Month YY"; only in DMY mode
		//           ^^
		// WARNING: "YY Month DD"; OK in YMD mode. In other modes, we'll
		//           ^^            wind up storing the year in the day.
		//                         This is fixed up below.
		// The month has been set, but we don't yet have a year. If we know
		// that the month was set in the first phase, we'll look for an
		// obvious year or defer to the parsing mode.
		if textMonth && (chunk.magnitude >= 3 || fe.mode == ParseModeYMD) {
			if chunk.magnitude <= 2 {
				fe.tweakYear = true
			}
			return fe.SetChunk(fieldYear, chunk)
		}
		return fe.SetChunk(fieldDay, chunk)

	case !fe.Wants(fieldYear) && !fe.Wants(fieldMonth) && fe.Wants(fieldDay):
		// Example: "YY MM DD"
		//                 ^^
		// WARNING: "DD MM YYYY"; may have mis-parsed the day value
		//           ^^    ^^^^
		// WARNING: "DD Month YYYY"; may have mis-parsed the day value
		//           ^^       ^^^^
		// Example: "YY Month DD"
		//                    ^^
		// Example: "YYYY MM DD"
		//                   ^^
		// Example: "YYYY Month DD"
		//                     ^^
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
		return fe.SetChunk(fieldDay, chunk)

	case fe.Wants(fieldYear) && fe.Wants(fieldMonth) && !fe.Wants(fieldDay):
		// Example: "DD MM YY"
		//              ^^
		// Example: "DD MM YYYY"
		//              ^^
		// BUT NOT: "Month DD YYYY"; text month set in first pass
		return fe.SetChunk(fieldMonth, chunk)

	case fe.Wants(fieldYear) && !fe.Wants(fieldMonth) && !fe.Wants(fieldDay):
		// Example: "MM DD YY"
		//                 ^^
		// Example: "MM DD YYYY"
		//                 ^^^^
		// Handle MDY, DMY formats.
		if chunk.magnitude <= 2 {
			fe.tweakYear = true
		}
		return fe.SetChunk(fieldYear, chunk)

	case fe.Wants(fieldTZHour) && (chunk.separator == '-' || chunk.separator == '+'):
		// Example: "<Time> +04[:05:06]"
		//                  ^^^
		// Example: "<Time> +0830"
		//                  ^^^^^
		// Example: "<Time> +083000"
		//                  ^^^^^^^
		// Example: "<Date> +08[:05:06]"; weird, but allowable.
		//                  ^^^
		// Example: "<Date> +0830"; weird, but allowable.
		//                  ^^^^^
		// Example: "<Date> +083000"; weird, but allowable.
		//                  ^^^^^^^
		// We're looking at a chunk that begins with a + or -.  We
		// know that it's not a YYY-MM-DD value, since all date
		// extract is handled in the previous cases. Since these
		// separators don't occur for the HH fields, it must be
		// a timezone.

		if chunk.separator == '-' {
			fe.tzSign = -1
		} else {
			fe.tzSign = 1
		}

		seconds := 0
		switch chunk.magnitude {
		case 1, 2:
			// H or HH
			return fe.SetChunk(fieldTZHour, chunk)
		case 6:
			// HHMMSS
			seconds = chunk.v % 100
			chunk.v /= 100
			fallthrough
		case 4:
			// HHMM, but either force seconds to 0 or use value from above.
			if err := fe.Set(fieldTZSecond, seconds); err != nil {
				return err
			}
			if err := fe.Set(fieldTZMinute, chunk.v%100); err != nil {
				return err
			}
			chunk.v /= 100
			return fe.SetChunk(fieldTZHour, chunk)

		default:
			return inputErrorf("unexpected number of digits for timezone in: %s", chunk)
		}

	case !fe.Wants(fieldTZHour) && fe.Wants(fieldTZMinute):
		// Example: "<Time> +04:05[:06]"
		//                      ^^
		// Example: "<Date> +08:05[:06]"; weird, but allowable.
		//                      ^^
		// BUT NOT: "<Date> +08 HH:MM:SS"
		// BUT NOT: "<Date> +08 HHMMSS"
		// If we have the first part of a timezone, we're either going
		// to see the TZ-minutes field or an HH field next.  We can
		// distinguish because the TZ-minutes field must have a
		// colon separator.
		if chunk.separator == ':' {
			return fe.SetChunk(fieldTZMinute, chunk)
		}

		// The separator wasn't a colon, so we no longer except to see any
		// timezone data. Mark the tz as completed and re-interpret the
		// chunk.  This retry only happens for valid inputs when the
		// timezone is in the middle of a timestamp.
		fe.has = fe.has.AddAll(tzFields)
		fe.wanted = fe.wanted.ClearAll(tzFields)
		return fe.interpretNumber(numbers, idx, textMonth)

	case !fe.Wants(fieldTZHour) && !fe.Wants(fieldTZMinute) && fe.Wants(fieldTZSecond):
		// Example: "<Time> +04:05:06"
		//                         ^^
		// Example: "<Date> +08:05:06"; weird, but allowable.
		//                         ^^
		// BUT NOT: "<Date> +08:30 HH:MM:SS"
		// BUT NOT: "<Date> +08:30 HHMMSS"
		// This case is exactly as the one above.
		if chunk.separator == ':' {
			return fe.SetChunk(fieldTZSecond, chunk)
		}

		// See the case above.
		fe.has = fe.has.Add(fieldTZSecond)
		fe.wanted = fe.wanted.Clear(fieldTZSecond)
		return fe.interpretNumber(numbers, idx, textMonth)

	case fe.Wants(fieldHour) && fe.Wants(fieldMinute) && fe.Wants(fieldSecond):
		// Example: "[Date] HH:MM:SS"
		//                  ^^
		// Example: "[Date] HHMM"
		//                  ^^^^
		// Example: "[Date] HHMMSS"
		//                  ^^^^^^
		// We're no longer looking for date fields at this point, and
		// we didn't match on a separator for a timezone component.
		// We must be looking at an hour or packed time field.
		seconds := 0
		switch chunk.magnitude {
		case 1, 2:
			// H or HH
			return fe.SetChunk(fieldHour, chunk)
		case 6:
			// HHMMSS
			seconds = chunk.v % 100
			chunk.v /= 100
			fallthrough
		case 4:
			// HHMM, but either force seconds to 0 or use value from above
			if err := fe.Set(fieldSecond, seconds); err != nil {
				return err
			}
			if err := fe.Set(fieldMinute, chunk.v%100); err != nil {
				return err
			}
			chunk.v /= 100
			return fe.SetChunk(fieldHour, chunk)

		default:
			return inputErrorf("unexpected number of digits for time in %v", chunk)
		}

	case fe.Wants(fieldMinute):
		// Example: "HH:MM"
		//              ^^
		return fe.SetChunk(fieldMinute, chunk)

	case fe.Wants(fieldSecond):
		// Example: "HH:MM:SS"
		//                 ^^
		return fe.SetChunk(fieldSecond, chunk)
	}
	return inputErrorf("could not parse field: %v", chunk)
}

// MakeDate returns a time.Time containing only the date components
// of the extract.
func (fe *fieldExtract) MakeDate() (Date, error) {
	if fe.sentinel != nil {
		switch *fe.sentinel {
		case TimeInfinity:
			return PosInfDate, nil
		case TimeNegativeInfinity:
			return NegInfDate, nil
		}
		return MakeDateFromTime(*fe.sentinel)
	}

	year, _ := fe.Get(fieldYear)
	month, _ := fe.Get(fieldMonth)
	day, _ := fe.Get(fieldDay)
	return MakeDateFromTime(time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC))
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

// Certain keywords should result in some kind of sentinel value,
// but we want to ensure that we accept only a single sentinel chunk.
func (fe *fieldExtract) matchedSentinel(value time.Time, match string) error {
	if fe.sentinel != nil {
		return inputErrorf("unexpected input: %s", match)
	}
	fe.sentinel = &value
	return nil
}

// Reset replaces a value of an already-set field.
func (fe *fieldExtract) Reset(field field, v int) error {
	if !fe.has.Has(field) {
		return errors.AssertionFailedf("field %s is not already set", errors.Safe(field.Pretty()))
	}
	fe.data[field] = v
	return nil
}

// Set updates the value of the given field, returning an error if
// the field has already been set.
func (fe *fieldExtract) Set(field field, v int) error {
	if !fe.wanted.Has(field) {
		return errors.AssertionFailedf("field %s is not wanted in %v", errors.Safe(field.Pretty()), errors.Safe(fe.wanted))
	}
	fe.data[field] = v
	fe.has = fe.has.Add(field)
	fe.wanted = fe.wanted.Clear(field)

	return nil
}

// SetChunk first validates that the separator in the chunk is appropriate
// for the field being set.  That is, you'd never see "YYYY:MM:DD";
// colons are only appropriate for hours and minutes.
func (fe *fieldExtract) SetChunk(field field, chunk numberChunk) error {
	// A space rune will be used for the initial chunk.
	switch field {
	case fieldYear:
		switch chunk.separator {
		case '-', '/', ' ', ',':
			// DD-MM-YY
			// DD/MM/YY
			// DD MM YY
			// Month DD, YYYY
			return fe.Set(field, chunk.v)
		}
	case fieldMonth, fieldDay:
		switch chunk.separator {
		case '-', '/', ' ':
			// DD-MM-YY
			// DD/MM/YY
			// DD MM YY
			// Month DD, YYYY
			return fe.Set(field, chunk.v)
		}
	case fieldTZHour:
		switch chunk.separator {
		case '-', '+':
			// HH:MM:SS+04
			// HH:MM:SS-04
			return fe.Set(field, chunk.v)
		}
	case fieldHour:
		switch chunk.separator {
		case ' ', 't':
			// YYYY-MM-DD HH:MM:SS
			// yyyymmddThhmmss
			return fe.Set(field, chunk.v)
		}
	case fieldMinute, fieldSecond, fieldTZMinute, fieldTZSecond:
		if chunk.separator == ':' {
			// HH:MM:SS
			return fe.Set(field, chunk.v)
		}
	case fieldNanos:
		if chunk.separator == '.' {
			// HH:MM:SS.NNNNNNNNN
			return fe.Set(field, chunk.v)
		}
	}
	return badFieldPrefixError(field, chunk.separator)
}

// SetDayOfYear updates the month and day fields to reflect the
// given day-of-year.  The year must have been previously set.
func (fe *fieldExtract) SetDayOfYear(chunk numberChunk) error {
	if chunk.separator != ' ' && chunk.separator != '.' {
		return badFieldPrefixError(fieldMonth, chunk.separator)
	}

	y, ok := fe.Get(fieldYear)
	if !ok {
		return errors.AssertionFailedf("year must be set before day of year")
	}
	y, m, d := julianDayToDate(dateToJulianDay(y, 1, 1) + chunk.v - 1)
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
			ret += fmt.Sprintf("%s: %d ", f.Pretty(), v)
		}
	}
	ret += "]"
	return ret
}

// validate ensures that the data in the extract is reasonable. It also
// performs some field fixups, such as converting two-digit years
// to actual values and adjusting for AM/PM.
func (fe *fieldExtract) validate() error {
	// If we have any of the required fields, we must have all of the required fields.
	if fe.has.HasAny(dateRequiredFields) && !fe.has.HasAll(dateRequiredFields) {
		return inputErrorf("missing required date fields")
	}
	if fe.has.HasAny(timeRequiredFields) && !fe.has.HasAll(timeRequiredFields) {
		return inputErrorf("missing required time fields")
	}
	if !fe.has.HasAll(fe.required) {
		return inputErrorf("missing required fields in input")
	}

	if year, ok := fe.Get(fieldYear); ok {
		// Note that here we allow for year to be 0 (which means 1 BC) which is
		// a deviation from Postgres. The issue is that we support two notations
		// (numbers or numbers with AD/BC suffix) whereas Postgres supports only
		// the latter.

		if era, ok := fe.Get(fieldEra); ok {
			if year <= 0 {
				return inputErrorf("only positive years are permitted in AD/BC notation")
			}
			if era < 0 {
				// Update for BC dates.
				if err := fe.Reset(fieldYear, 1-year); err != nil {
					return err
				}
			}
		} else if fe.tweakYear {
			if year < 0 {
				return inputErrorf("negative year not allowed")
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
				return outOfRangeError("month", month)
			}

			if day, ok := fe.Get(fieldDay); ok {
				var maxDay int
				if isLeap(year) {
					maxDay = daysInMonth[1][month]
				} else {
					maxDay = daysInMonth[0][month]
				}
				if day < 1 || day > maxDay {
					return outOfRangeError("day", day)
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
				return outOfRangeError("hour", hour)
			case hour == 12:
				if err := fe.Reset(fieldHour, 0); err != nil {
					return err
				}
			}

		case fieldValuePM:
			switch {
			case hour < 0 || hour > 12:
				return outOfRangeError("hour", hour)
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
				return outOfRangeError("hour", hour)
			}
		}

		minute, _ := fe.Get(fieldMinute)
		if minute < 0 || minute > 59 {
			return outOfRangeError("minute", minute)
		}

		second, _ := fe.Get(fieldSecond)
		if second < 0 || (hasDate && second > 60) || (!hasDate && second > 59) {
			return outOfRangeError("second", second)
		}

		nanos, _ := fe.Get(fieldNanos)
		if nanos < 0 {
			return outOfRangeError("nanos", nanos)
		}

		x := time.Duration(hour)*time.Hour +
			time.Duration(minute)*time.Minute +
			time.Duration(second)*time.Second +
			time.Duration(nanos)*time.Nanosecond
		if x > 24*time.Hour {
			return inputErrorf("time out of range: %d", x)
		}
	}

	return nil
}

// Wants returns whether or not the field is wanted in the extract.
func (fe *fieldExtract) Wants(field field) bool {
	return fe.wanted.Has(field)
}
