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
	"time"

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
	keywordInfinity  = "infinity"
	keywordNow       = "now"
	keywordPM        = "pm"
	keywordToday     = "today"
	keywordTomorrow  = "tomorrow"
	keywordYesterday = "yesterday"
)

//go:generate stringer -type=ParseMode

// ParseMode controls the resolution of ambiguous date formats such as
// `01/02/03`.
type ParseMode uint

const (
	// ParseModeISO is the default mode.
	ParseModeISO ParseMode = iota
	ParseModeYMD
	ParseModeDMY
	ParseModeMDY
)

// ParseDate converts a string into a time value.
func ParseDate(ctx Context, s string) (time.Time, error) {
	fe, err := newFieldExtract(ctx, s, dateFields, dateRequiredFields)
	if err != nil {
		if !ctx.exposeErr {
			err = errors.Errorf("could not parse date: %s", s)
		}
		return time.Time{}, err
	}
	year, _ := fe.Get(fieldYear)
	month, _ := fe.Get(fieldMonth)
	day, _ := fe.Get(fieldDay)
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, ctx.now().Location()), nil
}
