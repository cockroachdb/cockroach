// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgdate

import (
	"strconv"
	"strings"
	"time"
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

func init() {
	// Register a setter for every PostgreSQL-known fixed-offset abbreviation
	// (see pg_timezone_abbrevs.go). Pre-existing entries in keywordSetters
	// (UTC, GMT, Z, ZULU) take precedence: their setters use time.UTC, which
	// is equivalent for those four abbreviations and avoids churn.
	for abbrev, entry := range pgTimezoneAbbrevs {
		lower := strings.ToLower(abbrev)
		if _, exists := keywordSetters[lower]; exists {
			continue
		}
		keywordSetters[lower] = fieldSetterAbbreviation(abbrev, int(entry.UTCOffsetSecs))
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

	year, month, day := JulianDayToDate(date)

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

// fieldSetterAbbreviation returns a fieldSetter that resolves a PostgreSQL
// timezone abbreviation (e.g. EST, PST) to a fixed-offset time.Location.
// PostgreSQL's parsing path consults its session timezone first via
// TimeZoneAbbrevIsKnown before falling back to pg_timezone_abbrevs; CRDB
// does not currently emulate that precedence, so the fixed offset is always
// used. In practice this matches PostgreSQL for the modern era because the
// IANA zone reports the same offset as the abbreviation.
func fieldSetterAbbreviation(name string, offsetSecs int) fieldSetter {
	loc := time.FixedZone(name, offsetSecs)
	return func(fe *fieldExtract, _ string) error {
		fe.location = loc
		fe.wanted = fe.wanted.ClearAll(tzFields)
		return nil
	}
}
