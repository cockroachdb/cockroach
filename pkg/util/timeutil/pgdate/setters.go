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

	"github.com/pkg/errors"
)

type fieldSetter func(ctx Context, p *fieldExtract, s string) error

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
}

func fieldSetterAllBalls(_ Context, p *fieldExtract, _ string) error {
	return p.Zero(timeFields)
}

func fieldSetterExact(field field, v int) fieldSetter {
	return func(_ Context, p *fieldExtract, s string) error {
		return p.Set(field, v)
	}
}

func fieldSetterJulianDate(p *fieldExtract, s string) error {
	if !strings.HasPrefix(s, "j") {
		return errors.Errorf("unrecognized julian date: %s", s)
	}
	date, err := strconv.Atoi(s[1:])
	if err != nil {
		return errors.Wrap(err, "could not parse julian date")
	}

	year, month, day := julianDayToDate(date)

	if err := p.Set(fieldYear, year); err != nil {
		return err
	}
	if err := p.Set(fieldMonth, month); err != nil {
		return err
	}
	return p.Set(fieldDay, day)
}

func fieldSetterMonth(month int) fieldSetter {
	return fieldSetterExact(fieldMonth, month)
}

func fieldSetterRelativeDate(ctx Context, p *fieldExtract, s string) error {
	var offset int
	switch s {
	case keywordYesterday:
		offset = -1
	case keywordToday:
	case keywordTomorrow:
		offset = 1
	}

	year, month, day := ctx.now().AddDate(0, 0, offset).Date()

	if err := p.Set(fieldYear, year); err != nil {
		return err
	}
	if err := p.Set(fieldMonth, int(month)); err != nil {
		return err
	}
	return p.Set(fieldDay, day)
}
