// Copyright 2016 The Cockroach Authors.
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
//
// Author: Karl Southern (karl@theangryangel.co.uk)

package parser

import (
	"errors"
	"strings"
	"time"

	"fmt"
)

// Consumes the next integer from a string.
// Originally lifted directly from time/format.go and renamed from leadingInt,
// now ported to use slices, rather than allocating strings.
func consumeInt(s string, offset int) (x int64, rem int, err error) {
	i := offset
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			break
		}
		if x > (1<<63-1)/10 {
			// overflow
			return 0, 0, errors.New("interval: bad [0-9]*")
		}
		x = x*10 + int64(s[i]) - '0'
		if x < 0 {
			// overflow
			return 0, 0, errors.New("interval: bad [0-9]*")
		}
	}
	return x, i, nil
}

// Much like consumeInt, this consumes the next unit from the string.
func consumeUnit(s string, offset int, skipCharacter byte) (unit string, rem int, err error) {
	i := offset

	for ; i < len(s); i++ {
		if (s[i] >= '0' && s[i] <= '9') || s[i] == skipCharacter {
			break
		}
	}

	if s[offset:i] == "" {
		return "", 0, errors.New("interval: missing unit")
	}

	return s[offset:i], i, nil
}

// ISO Units.
var isoDateUnitMap = map[string]int64{
	"D": int64(time.Hour) * 24,
	"M": int64(time.Hour) * 24 * 30,       // ISO 8601 suggests we use 30 days
	"Y": int64(time.Hour) * 24 * 30 * 365, // Doesn't account for DST, or leap seconds
}

var isoTimeUnitMap = map[string]int64{
	"S": int64(time.Second),
	"M": int64(time.Minute),
	"H": int64(time.Hour),
}

// Parses a iso8601 (with designators) string.
// See the following links for examples:
//  - http://www.postgresql.org/docs/9.1/static/datatype-datetime.html#DATATYPE-INTERVAL-INPUT-EXAMPLES
//  - https://en.wikipedia.org/wiki/ISO_8601#Time_intervals
//  - https://en.wikipedia.org/wiki/ISO_8601#Durations
func iso8601ToDuration(s string) (time.Duration, error) {
	if s == "" || s[0] != 'P' {
		return 0, fmt.Errorf("interval: invalid iso8601 duration %s", s)
	}

	// Advance to offset 1, since we don't care about the leading P.
	offset := 1

	inDate := true

	var d int64

	for s[offset:] != "" {
		var (
			v   int64
			u   string
			err error
		)

		// Check if we're in the time part yet.
		if s[offset] == 'T' {
			inDate = false
			offset++
		}

		// Next character must be 0-9
		if s[offset] < '0' || s[offset] > '9' {
			return 0, fmt.Errorf("interval: invalid next character %c in iso8601 duration %s", s[offset], s)
		}

		v, offset, err = consumeInt(s, offset)
		if err != nil {
			return 0, err
		}

		u, offset, err = consumeUnit(s, offset, 'T')
		if err != nil {
			return 0, err
		}

		var (
			unit int64
			ok   bool
		)

		if inDate == true {
			unit, ok = isoDateUnitMap[u]
		} else {
			unit, ok = isoTimeUnitMap[u]
		}

		if !ok {
			return 0, fmt.Errorf("interval: unknown unit %s in iso8601 duration %s", u, s)
		}

		d += v * unit
	}

	return time.Duration(d), nil
}

// Postgres Units.
var postgresUnitMap = map[string]int64{
	"second": int64(time.Second),
	"minute": int64(time.Minute),
	"hour":   int64(time.Hour),
	"day":    int64(time.Hour) * 24,
	"month":  int64(time.Hour) * 24 * 30,       // Following postgres' example, we use 30 days
	"year":   int64(time.Hour) * 24 * 30 * 365, // Also like postgres, doesn't account for DST, leap seconds, or leap years
}

func postgresToDuration(s string) (time.Duration, error) {
	offset := 0
	s = strings.Replace(s, " ", "", -1)

	var d int64

	for s[offset:] != "" {
		var (
			v   int64
			u   string
			err error
		)

		// Next character must be valid.
		if s[offset] < '0' || s[offset] > '9' {
			return 0, fmt.Errorf("interval: invalid next character %c in postgres duration %s", s[offset], s)
		}

		v, offset, err = consumeInt(s, offset)
		if err != nil {
			return 0, err
		}

		u, offset, err = consumeUnit(s, offset, ' ')
		if err != nil {
			return 0, err
		}

		// Remove the trailing 's', so it can be found in the unitmap.
		if len(u) > 1 && u[len(u)-1] == 's' {
			u = u[:len(u)-1]
		}

		unit, ok := postgresUnitMap[u]

		if !ok {
			return 0, fmt.Errorf("interval: unknown unit %s in postgres duration %s", u, s)
		}

		d += v * unit
	}

	return time.Duration(d), nil
}
