// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package flagutil facilitates creation of rich flag types.
package flagutil

import (
	"fmt"
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/pflag"
)

// TimeFormats are the time formats which a time flag accepts for parsing time
// as described in time.Parse.
var TimeFormats = []string{
	log.MessageTimeFormat,
	log.FileTimeFormat,
	time.RFC3339Nano,
	time.RFC3339,
	time.Kitchen,
	"15:04:00.999999Z",
	"15:04:00.999999",
	"15:04Z",
	"15:04",
}

// Time returns a value which can be used with pflag.Var to create flags for
// time variables. The flag attempts to parse its in a variety of formats.
// The first is as a duration using time.ParseDuration and subtracting the
// result from Now() and then by using the values in Formats for parsing using
// time.Parse. An empty flag sets t to the zero value.
func Time(t *time.Time) pflag.Value {
	return (*timeFlag)(t)
}

// Regexp returns a value which can be used with pflag.Var to create flags
// for regexp variables. The flag attempts to compile its input to a regular
// expression and assigns the result to *r.
// If the flag is empty, r is set to nil.
func Regexp(r **regexp.Regexp) pflag.Value {
	return re{re: r}
}

// timeFlag implements pflag.Value and enables easy creation of time flags.
type timeFlag time.Time

func (f *timeFlag) String() string {
	if (*time.Time)(f).IsZero() {
		return ""
	}
	return (*time.Time)(f).Format(log.MessageTimeFormat)
}

func (f *timeFlag) Type() string { return "time" }

func (f *timeFlag) Set(s string) error {
	if s == "" {
		*f = (timeFlag)(time.Time{})
		return nil
	}
	for _, p := range parsers {
		if t, err := p.parseTime(s); err == nil {
			*f = timeFlag(t.UTC())
			return nil
		}
	}
	return fmt.Errorf("failed to parse %q as time", s)
}

var parsers = func() (parsers []interface {
	parseTime(s string) (time.Time, error)
}) {
	parsers = append(parsers, durationParser(time.ParseDuration))
	for _, p := range TimeFormats {
		parsers = append(parsers, formatParser(p))
	}
	return parsers
}()

type formatParser string

func (f formatParser) parseTime(s string) (time.Time, error) {
	return time.Parse(string(f), s)
}

type durationParser func(string) (time.Duration, error)

func (f durationParser) parseTime(s string) (time.Time, error) {
	d, err := f(s)
	if err != nil {
		return time.Time{}, err
	}
	return timeutil.Now().Add(-1 * d), nil
}

type re struct {
	re **regexp.Regexp
}

func (r re) String() string {
	if *r.re == nil {
		return ""
	}
	return (*r.re).String()
}

func (r re) Set(s string) error {
	if s == "" {
		*r.re = nil
		return nil
	}
	re, err := regexp.Compile(s)
	if err != nil {
		return err
	}
	*r.re = re
	return nil
}

func (r re) Type() string { return "regexp" }
