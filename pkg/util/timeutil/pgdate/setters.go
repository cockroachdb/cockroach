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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/pkg/errors"
)

type fieldSetter func(p *fieldExtract, s string) error

var keywordSetters = map[string]fieldSetter{
	"jan":       fieldSetterMonth(1),
	"january":   fieldSetterMonth(1),
	"feb":       fieldSetterMonth(2),
	"february":  fieldSetterMonth(2),
	"mar":       fieldSetterMonth(3),
	"march":     fieldSetterMonth(3),
	"apr":       fieldSetterMonth(4),
	"april":     fieldSetterMonth(4),
	"may":       fieldSetterMonth(5),
	"jun":       fieldSetterMonth(6),
	"june":      fieldSetterMonth(6),
	"jul":       fieldSetterMonth(7),
	"july":      fieldSetterMonth(7),
	"aug":       fieldSetterMonth(8),
	"august":    fieldSetterMonth(8),
	"sep":       fieldSetterMonth(9),
	"sept":      fieldSetterMonth(9), /* 4-chars, too in pg */
	"september": fieldSetterMonth(9),
	"oct":       fieldSetterMonth(10),
	"october":   fieldSetterMonth(10),
	"nov":       fieldSetterMonth(11),
	"november":  fieldSetterMonth(11),
	"dec":       fieldSetterMonth(12),
	"december":  fieldSetterMonth(12),

	keywordYesterday: fieldSetterRelativeDate,
	keywordToday:     fieldSetterRelativeDate,
	keywordTomorrow:  fieldSetterRelativeDate,

	keywordAllBalls: fieldSetterAllBalls,

	keywordEraAD: fieldSetterExact(fieldEra, fieldValueCE),
	keywordEraCE: fieldSetterExact(fieldEra, fieldValueCE),

	keywordEraBC:  fieldSetterExact(fieldEra, fieldValueBCE),
	keywordEraBCE: fieldSetterExact(fieldEra, fieldValueBCE),

	keywordAM: fieldSetterExact(fieldMeridian, fieldValueAM),
	keywordPM: fieldSetterExact(fieldMeridian, fieldValuePM),

	keywordGMT:  fieldSetterUTC,
	keywordUTC:  fieldSetterUTC,
	keywordZ:    fieldSetterUTC,
	keywordZulu: fieldSetterUTC,

	// TODO(bob, knz): Do we want to support the abbreviations in
	// https://github.com/postgres/postgres/blob/master/src/timezone/known_abbrevs.txt
}

// These abbreviations are taken from:
// https://github.com/postgres/postgres/blob/master/src/timezone/known_abbrevs.txt
// We have not yet implemented PostgreSQL's abbreviation-matching logic
// because we'd also need to incorporate more tzinfo than is readily-available
// from the time package.  Instead, we have this map to provide a more
// useful error message (and telemetry) until we do implement this
// behavior.
var unsupportedAbbreviations = [...]string{
	"ACDT", "ACST", "ADT", "AEDT", "AEST", "AKDT", "AKST", "AST", "AWST", "BST",
	"CAT", "CDT", "CDT", "CEST", "CET", "CST", "CST", "CST", "ChST",
	"EAT", "EDT", "EEST", "EET", "EST",
	// GMT has been removed from this list.
	"HDT", "HKT", "HST", "IDT", "IST", "IST", "IST", "JST", "KST",
	"MDT", "MEST", "MET", "MSK", "MST", "NDT", "NST", "NZDT", "NZST",
	"PDT", "PKT", "PST", "PST", "SAST", "SST", "UCT",
	// UTC has been removed from this list.
	"WAT", "WEST", "WET", "WIB", "WIT", "WITA",
}

func init() {
	for _, tz := range unsupportedAbbreviations {
		keywordSetters[strings.ToLower(tz)] = fieldSetterUnsupportedAbbreviation
	}
}

func fieldSetterAllBalls(p *fieldExtract, _ string) error {
	return p.Zero(timeFields)
}

func fieldSetterExact(field field, v int) fieldSetter {
	return func(p *fieldExtract, s string) error {
		p.Force(field, v)
		return nil
	}
}

func fieldSetterJulianDate(p *fieldExtract, s string) (bool, error) {
	if !strings.HasPrefix(s, "j") {
		return false, nil
	}
	date, err := strconv.Atoi(s[1:])
	if err != nil {
		return true, errors.Wrap(err, "could not parse julian date")
	}

	year, month, day := julianDayToDate(date)

	if err := p.Set(fieldYear, year); err != nil {
		return true, err
	}
	if err := p.Set(fieldMonth, month); err != nil {
		return true, err
	}
	return true, p.Set(fieldDay, day)
}

func fieldSetterMonth(month int) fieldSetter {
	return fieldSetterExact(fieldMonth, month)
}

func fieldSetterRelativeDate(p *fieldExtract, s string) error {
	var offset int
	switch s {
	case keywordYesterday:
		offset = -1
	case keywordToday:
	case keywordTomorrow:
		offset = 1
	}

	year, month, day := p.now.AddDate(0, 0, offset).Date()

	if err := p.Set(fieldYear, year); err != nil {
		return err
	}
	if err := p.Set(fieldMonth, int(month)); err != nil {
		return err
	}
	return p.Set(fieldDay, day)
}

// fieldSetterUTC forces the timezone to UTC and removes the
// TZ fields from the wanted list.
func fieldSetterUTC(p *fieldExtract, _ string) error {
	p.now = p.now.In(time.UTC)
	p.wanted = p.wanted.ClearAll(tzFields)
	return nil
}

func fieldSetterUnsupportedAbbreviation(_ *fieldExtract, _ string) error {
	return pgerror.UnimplementedWithIssueError(31710, "timestamp abbreviations not supported")
}
