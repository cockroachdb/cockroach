// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeutil

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	fixedOffsetPrefix = "fixed offset:"
	offsetBoundSecs   = 167*60*60 + 59*60
)

var timezoneOffsetRegex = regexp.MustCompile(`(?i)^(GMT|UTC)?([+-])?(\d{1,3}(:[0-5]?\d){0,2})$`)

// FixedOffsetTimeZoneToLocation creates a time.Location with a set offset and
// with a name that can be marshaled by crdb between nodes.
func FixedOffsetTimeZoneToLocation(offset int, origRepr string) *time.Location {
	return time.FixedZone(
		fmt.Sprintf("%s%d (%s)", fixedOffsetPrefix, offset, origRepr),
		offset)
}

// TimeZoneStringToLocationStandard is an option for the standard to use
// for parsing in TimeZoneStringToLocation.
type TimeZoneStringToLocationStandard uint32

const (
	// TimeZoneStringToLocationISO8601Standard parses int UTC offsets as *east* of
	// the GMT line, e.g. `-5` would be 'America/New_York' without daylight savings.
	TimeZoneStringToLocationISO8601Standard TimeZoneStringToLocationStandard = iota
	// TimeZoneStringToLocationPOSIXStandard parses int UTC offsets as *west* of the
	// GMT line, e.g. `+5` would be 'America/New_York' without daylight savings.
	TimeZoneStringToLocationPOSIXStandard
)

// TimeZoneStringToLocation transforms a string into a time.Location. It
// supports the usual locations and also time zones with fixed offsets created
// by FixedOffsetTimeZoneToLocation().
func TimeZoneStringToLocation(
	locStr string, std TimeZoneStringToLocationStandard,
) (*time.Location, error) {
	offset, origRepr, parsed := ParseFixedOffsetTimeZone(locStr)
	if parsed {
		return FixedOffsetTimeZoneToLocation(offset, origRepr), nil
	}

	// The time may just be a raw int value.
	intVal, err := strconv.ParseInt(locStr, 10, 64)
	if err == nil {
		// Parsing an int has different behavior for POSIX and ISO8601.
		if std == TimeZoneStringToLocationPOSIXStandard {
			intVal *= -1
		}
		return FixedOffsetTimeZoneToLocation(int(intVal)*60*60, locStr), nil
	}

	locTransforms := []func(string) string{
		func(s string) string { return s },
		strings.ToUpper,
		strings.ToTitle,
	}
	for _, transform := range locTransforms {
		if loc, err := LoadLocation(transform(locStr)); err == nil {
			return loc, nil
		}
	}

	tzOffset, ok := timeZoneOffsetStringConversion(locStr, std)
	if ok {
		return FixedOffsetTimeZoneToLocation(int(tzOffset), locStr), nil
	}
	return nil, errors.Newf("could not parse %q as time zone", locStr)
}

// ParseFixedOffsetTimeZone takes the string representation of a time.Location
// created by FixedOffsetTimeZoneToLocation and parses it to the offset and the
// original representation specified by the user. The bool returned is true if
// parsing was successful.
//
// The strings produced by FixedOffsetTimeZoneToLocation look like
// "<fixedOffsetPrefix><offset> (<origRepr>)".
// TODO(#42404): this is not the format given by the results in
// pgwire/testdata/connection_params.
func ParseFixedOffsetTimeZone(location string) (offset int, origRepr string, success bool) {
	if !strings.HasPrefix(location, fixedOffsetPrefix) {
		return 0, "", false
	}
	location = strings.TrimPrefix(location, fixedOffsetPrefix)
	parts := strings.SplitN(location, " ", 2)
	if len(parts) < 2 {
		return 0, "", false
	}

	offset, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", false
	}

	origRepr = parts[1]
	if !strings.HasPrefix(origRepr, "(") || !strings.HasSuffix(origRepr, ")") {
		return 0, "", false
	}
	return offset, strings.TrimSuffix(strings.TrimPrefix(origRepr, "("), ")"), true
}

// timeZoneOffsetStringConversion converts a time string to offset seconds.
// Supported time zone strings: GMT/UTC±[00:00:00 - 169:59:00].
// Seconds/minutes omittable and is case insensitive.
// By default, anything with a UTC/GMT prefix, or with : characters are POSIX.
// Whole integers can be POSIX or ISO8601 standard depending on the std variable.
func timeZoneOffsetStringConversion(
	s string, std TimeZoneStringToLocationStandard,
) (offset int64, ok bool) {
	submatch := timezoneOffsetRegex.FindStringSubmatch(strings.ReplaceAll(s, " ", ""))
	if len(submatch) == 0 {
		return 0, false
	}
	hasUTCPrefix := submatch[1] != ""
	prefix := submatch[2]
	timeString := submatch[3]

	var (
		hoursString   = "0"
		minutesString = "0"
		secondsString = "0"
	)
	offsets := strings.Split(timeString, ":")
	if strings.Contains(timeString, ":") {
		hoursString, minutesString = offsets[0], offsets[1]
		if len(offsets) == 3 {
			secondsString = offsets[2]
		}
	} else {
		hoursString = timeString
	}

	hours, _ := strconv.ParseInt(hoursString, 10, 64)
	minutes, _ := strconv.ParseInt(minutesString, 10, 64)
	seconds, _ := strconv.ParseInt(secondsString, 10, 64)
	offset = (hours * 60 * 60) + (minutes * 60) + seconds

	// GMT/UTC prefix, colons and POSIX standard characters have "opposite" timezones.
	if hasUTCPrefix || len(offsets) > 1 || std == TimeZoneStringToLocationPOSIXStandard {
		offset *= -1
	}
	if prefix == "-" {
		offset *= -1
	}

	if offset > offsetBoundSecs || offset < -offsetBoundSecs {
		return 0, false
	}
	return offset, true
}
