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
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// The functions in this file are used by fieldExtract.Extract().

// A fieldSetter is a helper function to set one or more
// fields within a fieldExtract in response to user input.
// These functions are used by fieldExtract.Extract().
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

	keywordEraAD: fieldSetterExact(fieldEra, fieldValueCE),
	keywordEraCE: fieldSetterExact(fieldEra, fieldValueCE),

	keywordEraBC:  fieldSetterExact(fieldEra, fieldValueBCE),
	keywordEraBCE: fieldSetterExact(fieldEra, fieldValueBCE),

	keywordAM: fieldSetterExact(fieldMeridian, fieldValueAM),
	keywordPM: fieldSetterExact(fieldMeridian, fieldValuePM),

	keywordAllBalls: fieldSetterUTC,
	keywordGMT:      fieldSetterUTC,
	keywordUTC:      fieldSetterUTC,
	keywordZ:        fieldSetterUTC,
	keywordZulu:     fieldSetterUTC,
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

// fieldSetterExact returns a fieldSetter that unconditionally sets field to v.
func fieldSetterExact(field field, v int) fieldSetter {
	return func(p *fieldExtract, _ string) error {
		return p.Set(field, v)
	}
}

// fieldSetterJulianDate parses a value like "J2451187" to set
// the year, month, and day fields.
func fieldSetterJulianDate(fe *fieldExtract, s string) (bool, error) {
	if !strings.HasPrefix(s, "j") {
		return false, nil
	}
	date, err := strconv.Atoi(s[1:])
	if err != nil {
		return true, inputErrorf("could not parse julian date")
	}

	year, month, day := julianDayToDate(date)

	if err := fe.Set(fieldYear, year); err != nil {
		return true, err
	}
	if err := fe.Set(fieldMonth, month); err != nil {
		return true, err
	}
	return true, fe.Set(fieldDay, day)
}

// fieldSetterMonth returns a fieldSetter that unconditionally sets
// the month to the given value.
func fieldSetterMonth(month int) fieldSetter {
	return fieldSetterExact(fieldMonth, month)
}

// fieldSetterRelativeDate sets the year, month, and day
// in response to the inputs "yesterday", "today", and "tomorrow"
// relative to fieldExtract.now.
func fieldSetterRelativeDate(fe *fieldExtract, s string) error {
	var offset int
	switch s {
	case keywordYesterday:
		offset = -1
	case keywordToday:
	case keywordTomorrow:
		offset = 1
	}

	year, month, day := fe.now().AddDate(0, 0, offset).Date()

	if err := fe.Set(fieldYear, year); err != nil {
		return err
	}
	if err := fe.Set(fieldMonth, int(month)); err != nil {
		return err
	}
	return fe.Set(fieldDay, day)
}

// fieldSetterUTC unconditionally sets the timezone to UTC and
// removes the TZ fields from the wanted list.
func fieldSetterUTC(fe *fieldExtract, _ string) error {
	fe.location = time.UTC
	fe.wanted = fe.wanted.ClearAll(tzFields)
	return nil
}

// fieldSetterUnsupportedAbbreviation always returns an error, but
// captures the abbreviation in telemetry.
func fieldSetterUnsupportedAbbreviation(_ *fieldExtract, s string) error {
	return unimplemented.NewWithIssueDetail(31710, s, "timestamp abbreviations not supported")
}
