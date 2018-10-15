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
	"unicode"
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
		return strconv.Itoa(n.v)
	} else {
		return string(n.prefix) + strconv.Itoa(n.v)
	}
}

// fieldExtract holds the results of parsing a date/time string.  Note
// that the enclosed fieldSet may have more entries in it than we have
// data.  This handles cases where a field, such as Julian date,
// would conflict with also setting the year.
type fieldExtract struct {
	data [fieldMaximum + 1]int
	has  fieldSet
	mode ParseMode
	// This indicates that the value in the year field was only
	// two digits and should be adjusted to make it recent.
	tweakYear bool
	// The fields that must be present to succeed.
	required fieldSet
	wanted   fieldSet
}

// These are sentinel values for handling special values:
// https://www.postgresql.org/docs/10/static/datatype-datetime.html#DATATYPE-DATETIME-SPECIAL-TABLE
var (
	sentinelEpoch            = (&fieldExtract{}).Freeze()
	sentinelInfinity         = (&fieldExtract{}).Freeze()
	sentinelNegativeInfinity = (&fieldExtract{}).Freeze()
	sentinelNow              = (&fieldExtract{}).Freeze()
)

var (
	dateFields         = newFieldSet(fieldYear, fieldMonth, fieldDay, fieldEra)
	dateRequiredFields = newFieldSet(fieldYear, fieldMonth, fieldDay)

	timeFields = newFieldSet(
		fieldHour, fieldMinute, fieldSecond, fieldFraction, fieldMeridian, fieldTZ1, fieldTZ2)
	timeRequiredFields = newFieldSet(fieldHour, fieldMinute)

	dateTimeFields         = dateFields.AddAll(timeFields)
	dateTimeRequiredFields = dateRequiredFields.AddAll(timeRequiredFields)
)

// newFieldExtract attempts to break the input string into a collection
// of date/time fields.  The toSet parameter controls which fields
// in the extract we will attempt to populate; there is no guarantee
// that all fields will necessary have been extracted.
func newFieldExtract(ctx Context, s string, toSet fieldSet, required fieldSet) (*fieldExtract, error) {
	ret := &fieldExtract{
		mode:     ctx.mode,
		required: required,
		wanted:   toSet,
	}

	chunks, _ := filterSplitString(s, func(r rune) bool {
		return unicode.IsDigit(r) || unicode.IsLetter(r)
	})

	numbers := make([]numberChunk, 0, len(chunks))
	sentinelMatched := false

	// First, we'll try to pluck out any keywords that exist in the input.
	// If a chunk is not a keyword or other special-case pattern, it
	// must be a numeric value, which we'll pluck out for a second
	// pass.
	for _, chunk := range chunks {
		match := strings.ToLower(chunk.Match)
		matchedSentinel := func() error {
			if sentinelMatched {
				return errors.Errorf("unexpected input: %s", match)
			}
			sentinelMatched = true
			return nil
		}

		switch match {
		case keywordEpoch:
			ret = sentinelEpoch
			if err := matchedSentinel(); err != nil {
				return nil, err
			}

		case keywordInfinity:
			if strings.HasSuffix(chunk.NotMatch, "-") {
				ret = sentinelNegativeInfinity
			} else {
				ret = sentinelInfinity
			}
			if err := matchedSentinel(); err != nil {
				return nil, err
			}

		case keywordNow:
			ret = sentinelNow
			if err := matchedSentinel(); err != nil {
				return nil, err
			}

		default:
			// Fan out to other keyword-based extracts.
			if m, ok := keywordSetters[match]; ok {
				if err := m(ctx, ret, match); err != nil {
					return nil, err
				}
				continue
			}

			// Try to parse Julian dates.  We can disregard the error here.
			if err := fieldSetterJulianDate(ret, match); err == nil {
				continue
			}

			// At this point, the chunk must be numeric; we'll convert it
			// and add it to the slice for the second pass.
			v, err := strconv.Atoi(match)
			if err != nil {
				return nil, errors.Errorf("unexpected input chunk: %s", match)
			}
			lastRune, _ := utf8.DecodeLastRuneInString(chunk.NotMatch)
			numbers = append(numbers, numberChunk{prefix: lastRune, v: v, magnitude: len(match)})
		}
	}

	// In the second pass, we'll use pattern-matching and the knowledge
	// of which fields have already been set in order to keep picking
	// out field data.
	textMonth := !ret.Wants(fieldMonth)
	for _, n := range numbers {
		if err := ret.interpretNumber(n, textMonth); err != nil {
			return nil, err
		}
	}

	if err := ret.Validate(); err != nil {
		return nil, err
	}

	return ret, nil
}

// Freeze clears the wanted fields set and returns the fieldExtract.
func (fe *fieldExtract) Freeze() *fieldExtract {
	fe.wanted = 0
	return fe
}

// Get returns the value of the requested field and whether or not
// that field has indeed been set.
func (fe *fieldExtract) Get(field field) (int, bool) {
	return fe.data[field], fe.has.Has(field)
}

func (fe *fieldExtract) interpretNumber(chunk numberChunk, textMonth bool) error {
	switch {
	case chunk.magnitude >= 6:
		// It's a run-together value, use alternate parsing strategy
		return fe.interpretPackedNumber(chunk)

	case chunk.prefix == '.':
		// It's either a yyyy.ddd or we're looking at fractions.
		switch {
		case !fe.Wants(fieldYear) && fe.Wants(fieldMonth) && fe.Wants(fieldDay):
			return fe.SetDayOfYear(chunk.v)

		case fe.Wants(fieldFraction):
			return fe.Set(fieldFraction, chunk.v)

		default:
			return errors.Errorf("cannot interpret %s", chunk)
		}

	case chunk.magnitude == 3 && !fe.Wants(fieldYear) && chunk.v >= 1 && chunk.v <= 366:
		// A three-digit value could be a day-of-year.
		return fe.SetDayOfYear(chunk.v)

	case fe.Wants(fieldYear) && fe.Wants(fieldMonth) && fe.Wants(fieldDay):
		// We're starting from scratch
		switch {
		case chunk.magnitude >= 3 || fe.mode == ParseModeYMD || fe.mode == ParseModeISO:
			if chunk.magnitude <= 2 {
				fe.tweakYear = true
			}
			return fe.Set(fieldYear, chunk.v)
		case fe.mode == ParseModeDMY:
			return fe.Set(fieldDay, chunk.v)
		default:
			return fe.Set(fieldMonth, chunk.v)
		}

	case !fe.Wants(fieldYear) && fe.Wants(fieldMonth) && fe.Wants(fieldDay):
		// Must be a year-month-day
		return fe.Set(fieldMonth, chunk.v)

	case fe.Wants(fieldYear) && !fe.Wants(fieldMonth) && fe.Wants(fieldDay):
		// The month has been set; this could be mm-dd-yy or a
		// format with a textual month entry.  If we know that the
		// month was set in the first phase, we'll look for an obvious year
		// or defer to the parsing mode.
		if textMonth && (chunk.magnitude >= 3 || fe.mode == ParseModeYMD) {
			if chunk.magnitude <= 2 {
				fe.tweakYear = true
			}
			return fe.Set(fieldYear, chunk.v)
		} else {
			return fe.Set(fieldDay, chunk.v)
		}

	case !fe.Wants(fieldYear) && !fe.Wants(fieldMonth) && fe.Wants(fieldDay):
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
		} else {
			return fe.Set(fieldDay, chunk.v)
		}

	case fe.Wants(fieldYear) && fe.Wants(fieldMonth) && !fe.Wants(fieldDay):
		// Must be looking at a dd-mm-yy format.
		return fe.Set(fieldMonth, chunk.v)

	case fe.Wants(fieldYear) && !fe.Wants(fieldMonth) && !fe.Wants(fieldDay):
		// Have month and day, last field must be a year.
		if chunk.magnitude <= 2 {
			fe.tweakYear = true
		}
		return fe.Set(fieldYear, chunk.v)
	default:
		return errors.Errorf("could not interpret %v", chunk)
	}
}

// InterpretPackedNumber is where we do the pattern-matching to
// ingest cases in the style of yyyymmdd or hhmmss.
func (fe *fieldExtract) interpretPackedNumber(chunk numberChunk) error {
	switch {
	case fe.Wants(fieldYear) && fe.Wants(fieldMonth) && fe.Wants(fieldDay):
		if chunk.magnitude >= 6 {
			v := chunk.v
			fe.tweakYear = chunk.magnitude == 6
			// yymmdd or yyyymmdd
			if err := fe.Set(fieldDay, v%100); err != nil {
				return err
			}
			v /= 100
			if err := fe.Set(fieldMonth, v%100); err != nil {
				return err
			}
			v /= 100
			return fe.Set(fieldYear, v)
		}

	case fe.Wants(fieldMinute):
		switch chunk.magnitude {
		case 6:
			// hhmmss
		case 4:
			// hhmm
		}
	}

	return errors.Errorf("could not interpret %v", chunk)
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
	if !fe.has.HasAll(fe.required) {
		return errors.Errorf("missing required fields %v; have %v", fe.required, fe.has)
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

// splitChunks are returned from filterSplitString.
type splitChunk struct {
	// The contiguous span of characters that did not match the filter and
	// which appear immediately before Match.
	NotMatch string
	// The contiguous span of characters that matched the filter.
	Match string
}

// filterSplitString filters the runes in a string and returns
// contiguous spans of acceptable characters.
func filterSplitString(s string, accept func(rune) bool) ([]splitChunk, string) {
	ret := make([]splitChunk, 0, 8)

	matchStart := 0
	matchEnd := 0
	previousMatchEnd := 0

	flush := func() {
		if matchEnd > matchStart {
			ret = append(ret, splitChunk{
				NotMatch: s[previousMatchEnd:matchStart],
				Match:    s[matchStart:matchEnd]})
			previousMatchEnd = matchEnd
			matchStart = matchEnd
		}
	}

	for offset, r := range s {
		if accept(r) {
			if matchStart >= matchEnd {
				matchStart = offset
			}
			matchEnd = offset + utf8.RuneLen(r)
		} else {
			flush()
		}
	}
	flush()

	return ret, s[matchEnd:]
}
