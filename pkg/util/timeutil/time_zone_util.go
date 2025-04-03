// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	offsetBoundSecs = 167*60*60 + 59*60
	// PG supports UTC hour offsets in the range [-167, 167].
	maxUTCHourOffset          = 167
	maxUTCHourOffsetInSeconds = maxUTCHourOffset * 60 * 60
)

var timezoneOffsetRegex = regexp.MustCompile(`(?i)^(GMT|UTC)?([+-])?(\d{1,3}(:[0-5]?\d){0,2})$`)

// FixedTimeZoneOffsetToLocation creates a time.Location with an offset and a
// time zone string.
func FixedTimeZoneOffsetToLocation(offset int, origRepr string) *time.Location {
	// The offset name always should be normalized to upper-case for UTC/GMT.
	return time.FixedZone(strings.ToUpper(origRepr), offset)
}

// TimeZoneOffsetToLocation takes an offset and name that can be marshaled by
// crdb between nodes and creates a time.Location.
// Note that the display time zone is always shown with ISO sign convention.
func TimeZoneOffsetToLocation(offset int) *time.Location {
	origRepr := secondsToHoursMinutesSeconds(offset)
	if offset <= 0 {
		origRepr = fmt.Sprintf("<-%s>+%s", origRepr, origRepr)
	} else {
		origRepr = fmt.Sprintf("<+%s>-%s", origRepr, origRepr)
	}

	return time.FixedZone(origRepr, offset)
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
// by FixedTimeZoneOffsetToLocation().
func TimeZoneStringToLocation(
	locStr string, std TimeZoneStringToLocationStandard,
) (*time.Location, error) {
	// ParseTimeZoneOffset uses strconv.ParseFloat, which returns an error when
	// parsing fails that is expensive to construct. We first check if the string
	// contains any non-numeric characters to see if we can skip attempting to
	// parse it as a timezone offset. `/` is also checked since that character
	// appears in most timezone names. Since UTC is the most commonly used
	// timezone, we also check for that explicitly to avoid calling ContainsAny if
	// possible.
	containsNonNumeric := strings.EqualFold(locStr, "utc") || strings.ContainsAny(locStr, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ/")
	if !containsNonNumeric {
		offset, _, parsed := ParseTimeZoneOffset(locStr, std)
		if parsed {
			if offset < -maxUTCHourOffsetInSeconds || offset > maxUTCHourOffsetInSeconds {
				return nil, errors.New("UTC timezone offset is out of range.")
			}
			return TimeZoneOffsetToLocation(offset), nil
		}
	}

	// The time may just be a raw int value. Similar to the above, in order to
	// avoid constructing an expensive error, we first check if the string
	// contains any non-numeric characters to see if we can skip attempting to
	// parse it as an int.
	if !containsNonNumeric {
		intVal, err := strconv.ParseInt(locStr, 10, 64)
		if err == nil {
			// Parsing an int has different behavior for POSIX and ISO8601.
			if std == TimeZoneStringToLocationPOSIXStandard {
				intVal *= -1
			}
			if intVal < -maxUTCHourOffset || intVal > maxUTCHourOffset {
				return nil, errors.New("UTC timezone offset is out of range.")
			}
			return TimeZoneOffsetToLocation(int(intVal) * 60 * 60), nil
		}
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
	if !ok {
		return nil, errors.Newf("could not parse %q as time zone", locStr)
	}
	return FixedTimeZoneOffsetToLocation(int(tzOffset), locStr), nil
}

// ParseTimeZoneOffset takes the string representation of a time.Location
// created by TimeZoneOffsetToLocation and parses it to the offset and the
// original representation specified by the user. The bool returned is true if
// parsing was successful.
// The offset is formatted <-%s>+%s or <+%s>+%s.
// A string with whitespace padding optionally followed by a (+/-)
// and a float should be able to be parsed. Example: "    +10.5" is parsed in
// PG and displayed as  <+10:06:36>-10:06:36.
func ParseTimeZoneOffset(
	location string, standard TimeZoneStringToLocationStandard,
) (offset int, origRepr string, success bool) {
	if strings.HasPrefix(location, "<") {
		// The string has the format <+HH:MM:SS>-HH:MM:SS or <-HH:MM:SS>+HH:MM:SS.
		// Parse the time between the < >.
		// Grab the time from between the < >.
		regexPattern, err := regexp.Compile(`\<[+-].*\>`)
		if err != nil {
			return 0, "", false
		}
		origRepr = regexPattern.FindString(location)
		origRepr = strings.TrimPrefix(origRepr, "<")
		origRepr = strings.TrimSuffix(origRepr, ">")

		offsetMultiplier := 1
		if strings.HasPrefix(origRepr, "-") {
			offsetMultiplier = -1
		}

		origRepr = strings.Trim(origRepr, "+")
		origRepr = strings.Trim(origRepr, "-")

		// Parse HH:MM:SS time.
		offset = hoursMinutesSecondsToSeconds(origRepr)
		offset *= offsetMultiplier

		return offset, location, true
	}

	// Try parsing the string in the format whitespaces optionally followed by
	// (+/-) followed immediately by a float.
	origRepr = strings.TrimSpace(location)
	origRepr = strings.TrimPrefix(origRepr, "+")

	multiplier := 1
	if strings.HasPrefix(origRepr, "-") {
		multiplier = -1
		origRepr = strings.TrimPrefix(origRepr, "-")
	}

	if standard == TimeZoneStringToLocationPOSIXStandard {
		multiplier *= -1
	}

	f, err := strconv.ParseFloat(origRepr, 64)
	if err != nil {
		return 0, "", false
	}

	origRepr = floatToHoursMinutesSeconds(f)
	offset = hoursMinutesSecondsToSeconds(origRepr)
	return multiplier * offset, origRepr, true
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

	offsets := strings.Split(timeString, ":")
	offset = int64(hoursMinutesSecondsToSeconds(timeString))

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

// The timestamp must be of one of the following formats:
//
//	HH
//	HH:MM
//	HH:MM:SS
func hoursMinutesSecondsToSeconds(timeString string) int {
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
	return int((hours * 60 * 60) + (minutes * 60) + seconds)
}

// secondsToHoursMinutesSeconds converts seconds to a timestamp of the format
//
//	HH
//	HH:MM
//	HH:MM:SS
func secondsToHoursMinutesSeconds(totalSeconds int) string {
	secondsPerHour := 60 * 60
	secondsPerMinute := 60
	if totalSeconds < 0 {
		totalSeconds = totalSeconds * -1
	}
	hours := totalSeconds / secondsPerHour
	minutes := (totalSeconds - hours*secondsPerHour) / secondsPerMinute
	seconds := totalSeconds - hours*secondsPerHour - minutes*secondsPerMinute

	if seconds == 0 && minutes == 0 {
		return fmt.Sprintf("%02d", hours)
	} else if seconds == 0 {
		return fmt.Sprintf("%d:%d", hours, minutes)
	} else {
		// PG doesn't round, truncate precision.
		return fmt.Sprintf("%d:%d:%2.0d", hours, minutes, seconds)
	}
}

// floatToHoursMinutesSeconds converts a float to a HH:MM:SS.
// The minutes and seconds sections are only included in the precision is
// necessary.
// For example:
//
//	11.00 -> 11
//	11.5 -> 11:30
//	11.51 -> 11:30:36
func floatToHoursMinutesSeconds(f float64) string {
	hours := int(f)
	remaining := f - float64(hours)

	secondsPerHour := float64(60 * 60)
	totalSeconds := remaining * secondsPerHour
	minutes := int(totalSeconds / 60)
	seconds := totalSeconds - float64(minutes*60)

	if seconds == 0 && minutes == 0 {
		return fmt.Sprintf("%02d", hours)
	} else if seconds == 0 {
		return fmt.Sprintf("%d:%d", hours, minutes)
	} else {
		// PG doesn't round, truncate precision.
		return fmt.Sprintf("%d:%d:%2.0f", hours, minutes, seconds)
	}
}
