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
	"unicode"
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

// extract attempts to break the input string into a collection
// of date/time fields in order to populate a fieldExtract.  The
// fieldExtract returned from this function may not be the one
// passed in if a sentinel value is returned. An error will be
// returned if not all of the required fields have been populated.
func extract(fe fieldExtract, s string) (time.Time, error) {
	returnDate := fe.required.HasAll(dateRequiredFields)
	returnTime := fe.required.HasAll(timeRequiredFields)

	// Break the string into alphanumeric chunks.
	textChunks, _ := chunk(s)

	// Create a place to store extracted numeric info.
	numbers := make([]numberChunk, 0, len(textChunks))
	appendNumber := func(prefix, number string) error {
		if v, err := strconv.Atoi(number); err == nil {
			lastRune, _ := utf8.DecodeLastRuneInString(prefix)
			numbers = append(numbers, numberChunk{prefix: lastRune, v: v, magnitude: len(number)})
			return nil
		} else {
			return err
		}
	}

	// Certain keywords should result in some kind of sentinel value,
	// but we want to ensure that we accept only a single sentinel chunk.
	var sentinel *time.Time
	matchedSentinel := func(value *time.Time, match string) error {
		if sentinel != nil {
			return errors.Errorf("unexpected input: %s", match)
		}
		sentinel = value
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
				return TimeEpoch, err
			}

		case keywordInfinity:
			if strings.HasSuffix(chunk.NotMatch, "-") {
				if err := matchedSentinel(&TimeNegativeInfinity, match); err != nil {
					return TimeEpoch, err
				}
			} else {
				if err := matchedSentinel(&TimeInfinity, match); err != nil {
					return TimeEpoch, err
				}
			}

		case keywordNow:
			if err := matchedSentinel(&fe.now, match); err != nil {
				return TimeEpoch, err
			}

		default:
			// Fan out to other keyword-based extracts.
			if m, ok := keywordSetters[match]; ok {
				if err := m(&fe, match); err != nil {
					return TimeEpoch, err
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
				if err := fieldSetterUTC(&fe, ""); err != nil {
					return TimeEpoch, err
				}
				maybeMatch := match[:len(match)-len(keywordZ)]
				if err := appendNumber(chunk.NotMatch, maybeMatch); err == nil {
					continue
				}

			} else if strings.HasSuffix(match, keywordZulu) {
				if err := fieldSetterUTC(&fe, ""); err != nil {
					return TimeEpoch, err
				}
				maybeMatch := match[:len(match)-len(keywordZulu)]
				if err := appendNumber(chunk.NotMatch, maybeMatch); err == nil {
					continue
				}
			}

			// Try to parse Julian dates.
			if matched, err := fieldSetterJulianDate(&fe, match); matched {
				if err != nil {
					return TimeEpoch, err
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

			// Remove TZ1 and TZ2 from the wanted list, but add a date
			// in order to resolve the location's DST.  Also, if we had a
			// text month, ensure that it's also not in the wanted field.
			fe.wanted = fe.wanted.AddAll(dateFields).ClearAll(fe.has.Add(fieldTZ1).Add(fieldTZ2))
		} else {
			return TimeEpoch, errors.Errorf("could not interpret leftovers: %s", leftoverText)
		}
	}

	if sentinel != nil {
		return *sentinel, nil
	}

	// In the second pass, we'll use pattern-matching and the knowledge
	// of which fields have already been set in order to keep picking
	// out field data.
	textMonth := !fe.Wants(fieldMonth)
	for _, n := range numbers {
		if fe.wanted == 0 {
			return TimeEpoch, errors.Errorf("too many input chunks")
		}
		if err := fe.interpretNumber(n, textMonth); err != nil {
			return TimeEpoch, err
		}
	}

	if err := fe.Validate(); err != nil {
		return TimeEpoch, err
	}

	var year, month, day, hour, min, sec, nano int

	if fe.has.HasAll(dateRequiredFields) {
		year, _ = fe.Get(fieldYear)
		month, _ = fe.Get(fieldMonth)
		day, _ = fe.Get(fieldDay)
	} else {
		year = 0
		month = 1
		day = 1
	}

	if returnDate && !returnTime {
		return time.Date(year, time.Month(month), day, 0, 0, 0, 0, fe.now.Location()), nil
	}

	var loc *time.Location
	if fe.has.HasAll(timeRequiredFields) {
		hour, _ = fe.Get(fieldHour)
		min, _ = fe.Get(fieldMinute)
		sec, _ = fe.Get(fieldSecond)
		nano, _ = fe.Get(fieldFraction)

		if tz1, ok := fe.Get(fieldTZ1); ok {
			tz2, _ := fe.Get(fieldTZ2)
			tz1 *= fe.tzSign
			tz2 *= fe.tzSign
			loc = zoneCacheInstance.FixedZone(tz1, tz2)
		} else {
			loc = fe.now.Location()
		}
	} else {
		loc = fe.now.Location()
	}

	ret := time.Date(year, time.Month(month), day, hour, min, sec, nano, loc)

	// We'll truncate back to the zero day if  the user didn't request
	// the date to be returned.  If the user provided a named timezone,
	// like America/New_York, then they will have also had to have
	// provided a date to resolve that name into an actual offset.
	// We'll convert that offset into a fixed timezone when we return
	// the date.
	if !returnDate && fe.has.HasAny(dateRequiredFields) {
		hour, min, sec := ret.Clock()
		_, offset := ret.Zone()
		ret = time.Date(0, 1, 1, hour, min, sec, ret.Nanosecond(), time.FixedZone("", offset))
	}

	return ret, nil
}

// stringChunk is returned by chunk().
type stringChunk struct {
	// The contiguous span of characters that did not match the filter and
	// which appear immediately before Match.
	NotMatch string
	// The contiguous span of characters that matched the filter.
	Match string
}

// chunk filters the runes in a string and returns
// contiguous spans of alphanumeric characters.
func chunk(s string) ([]stringChunk, string) {
	ret := make([]stringChunk, 0, 8)

	matchStart := 0
	matchEnd := 0
	previousMatchEnd := 0

	flush := func() {
		if matchEnd > matchStart {
			notMatch := s[previousMatchEnd:matchStart]
			match := s[matchStart:matchEnd]

			// Special-case to handle ddThh delimiter
			if len(match) == 5 && (match[2:3] == "T" || match[2:3] == "t") {
				ret = append(ret,
					stringChunk{
						NotMatch: notMatch,
						Match:    match[:2],
					},
					stringChunk{
						NotMatch: "T",
						Match:    match[3:],
					})
			} else {
				ret = append(ret, stringChunk{
					NotMatch: notMatch,
					Match:    match,
				})
			}
			previousMatchEnd = matchEnd
			matchStart = matchEnd
		}
	}

	for offset, r := range s {
		if unicode.IsDigit(r) || unicode.IsLetter(r) {
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
