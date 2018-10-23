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
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pkg/errors"
)

const (
	keywordAllBalls  = "allballs"
	keywordAM        = "am"
	keywordEpoch     = "epoch"
	keywordEraAD     = "ad"
	keywordEraBC     = "bc"
	keywordEraBCE    = "bce"
	keywordEraCE     = "ce"
	keywordGMT       = "gmt"
	keywordInfinity  = "infinity"
	keywordNow       = "now"
	keywordPM        = "pm"
	keywordToday     = "today"
	keywordTomorrow  = "tomorrow"
	keywordUTC       = "utc"
	keywordYesterday = "yesterday"
	keywordZ         = "z"
	keywordZulu      = "zulu"
)

// Extract attempts to break the input string into a collection
// of date/time fields in order to populate a fieldExtract.  The
// fieldExtract returned from this function may not be the one
// passed in if a sentinel value is returned. An error will be
// returned if not all of the required fields have been populated.
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
		if err := fe.interpretNumber(n, textMonth); err != nil {
			return err
		}
	}

	if err := fe.Validate(); err != nil {
		return err
	}

	return nil
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
	nano, _ := fe.Get(fieldFraction)

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
