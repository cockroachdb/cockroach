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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Various keywords that appear in timestamps.
const (
	// Alias for UTC.  No idea who actually uses this.
	keywordAllBalls = "allballs"
	keywordAM       = "am"
	keywordEpoch    = "epoch"
	keywordEraAD    = "ad"
	keywordEraBC    = "bc"
	keywordEraBCE   = "bce"
	keywordEraCE    = "ce"
	// Alias for UTC.
	keywordGMT       = "gmt"
	keywordInfinity  = "infinity"
	keywordNow       = "now"
	keywordPM        = "pm"
	keywordToday     = "today"
	keywordTomorrow  = "tomorrow"
	keywordUTC       = "utc"
	keywordYesterday = "yesterday"
	// Alias for UTC.
	keywordZ = "z"
	// Alias for UTC.
	keywordZulu = "zulu"
)

// Commonly-used collections of fields.
var (
	dateFields         = newFieldSet(fieldYear, fieldMonth, fieldDay, fieldEra)
	dateRequiredFields = newFieldSet(fieldYear, fieldMonth, fieldDay)

	timeFields = newFieldSet(
		fieldHour, fieldMinute, fieldSecond, fieldNanos, fieldMeridian,
		fieldTZHour, fieldTZMinute, fieldTZSecond)
	timeRequiredFields = newFieldSet(fieldHour, fieldMinute)

	db2TimeRequiredFields = newFieldSet(fieldHour, fieldMinute, fieldSecond)

	dateTimeFields = dateFields.AddAll(timeFields)

	tzFields = newFieldSet(fieldTZHour, fieldTZMinute, fieldTZSecond)
)

// These are sentinel values for handling special values:
// https://www.postgresql.org/docs/10/static/datatype-datetime.html#DATATYPE-DATETIME-SPECIAL-TABLE
var (
	TimeEpoch = timeutil.Unix(0, 0)
	// TimeInfinity represents the "highest" possible time.
	// TODO (#41564): this should actually behave as infinity, i.e. any operator
	// leaves this as infinity. This time should always be greater than any other time.
	// We should probably use the next microsecond after this value, i.e. timeutil.Unix(9224318016000, 0).
	// Postgres uses math.MaxInt64 microseconds as the infinity value.
	// See: https://github.com/postgres/postgres/blob/42aa1f0ab321fd43cbfdd875dd9e13940b485900/src/include/datatype/timestamp.h#L107.
	TimeInfinity = timeutil.Unix(9224318016000-1, 999999000)
	// TimeNegativeInfinity represents the "lowest" possible time.
	// TODO (#41564): this should actually behave as -infinity, i.e. any operator
	// leaves this as -infinity. This time should always be less than any other time.
	// We should probably use the next microsecond before this value, i.e. timeutil.Unix(9224318016000-1, 999999000).
	// Postgres uses math.MinInt64 microseconds as the -infinity value.
	// See: https://github.com/postgres/postgres/blob/42aa1f0ab321fd43cbfdd875dd9e13940b485900/src/include/datatype/timestamp.h#L107.
	TimeNegativeInfinity = timeutil.Unix(-210866803200, 0)
)

type ParseHelper struct {
	fe fieldExtract
}

// ParseDate converts a string into Date.
//
// Any specified timezone is inconsequential. Examples:
//   - "now": parses to the local date (in the current timezone)
//   - "2020-06-26 01:09:15.511971": parses to '2020-06-26'
//   - "2020-06-26 01:09:15.511971-05": parses to '2020-06-26'
//
// The dependsOnContext return value indicates if we had to consult the given
// `now` value (either for the time or the local timezone).
//
// Memory allocations can be avoided by passing ParseHelper which can be re-used
// across calls for batch parsing purposes, otherwise it can be nil.
func ParseDate(
	now time.Time, dateStyle DateStyle, s string, h *ParseHelper,
) (_ Date, dependsOnContext bool, _ error) {
	if h == nil {
		h = &ParseHelper{}
	}
	h.fe = fieldExtract{
		currentTime: now,
		dateStyle:   dateStyle,
		required:    dateRequiredFields,
		// We allow time fields to be provided since they occur after
		// the date fields that we're really looking for and for
		// time values like 24:00:00, would push into the next day.
		wanted: dateTimeFields,
	}

	if err := h.fe.Extract(s); err != nil {
		return Date{}, false, parseError(err, "date", s)
	}
	date, err := h.fe.MakeDate()
	return date, h.fe.currentTimeUsed, err
}

// ParseTime converts a string into a time value on the epoch day.
//
// The dependsOnContext return value indicates if we had to consult the given
// `now` value (either for the time or the local timezone).
//
// Memory allocations can be avoided by passing ParseHelper which can be re-used
// across calls for batch parsing purposes, otherwise it can be nil.
func ParseTime(
	now time.Time, dateStyle DateStyle, s string, h *ParseHelper,
) (_ time.Time, dependsOnContext bool, _ error) {
	if h == nil {
		h = &ParseHelper{}
	}
	h.fe = fieldExtract{
		currentTime: now,
		required:    timeRequiredFields,
		wanted:      timeFields,
	}

	if err := h.fe.Extract(s); err != nil {
		// It's possible that the user has given us a complete
		// timestamp string; let's try again, accepting more fields.
		h.fe = fieldExtract{
			currentTime: now,
			dateStyle:   dateStyle,
			required:    timeRequiredFields,
			wanted:      dateTimeFields,
		}

		if err := h.fe.Extract(s); err != nil {
			return TimeEpoch, false, parseError(err, "time", s)
		}
	}
	res := h.fe.MakeTime()
	return res, h.fe.currentTimeUsed, nil
}

// ParseTimeWithoutTimezone converts a string into a time value on the epoch
// day, dropping any timezone information. The returned time always has UTC
// location.
//
// Any specified timezone is inconsequential. Examples:
//   - "now": parses to the local time of day (in the current timezone)
//   - "01:09:15.511971" and "01:09:15.511971-05" parse to the same result
//
// The dependsOnContext return value indicates if we had to consult the given
// `now` value (either for the time or the local timezone).
func ParseTimeWithoutTimezone(
	now time.Time, dateStyle DateStyle, s string,
) (_ time.Time, dependsOnContext bool, _ error) {
	fe := fieldExtract{
		currentTime: now,
		required:    timeRequiredFields,
		wanted:      timeFields,
	}

	if err := fe.Extract(s); err != nil {
		// It's possible that the user has given us a complete
		// timestamp string; let's try again, accepting more fields.
		fe = fieldExtract{
			currentTime: now,
			dateStyle:   dateStyle,
			required:    timeRequiredFields,
			wanted:      dateTimeFields,
		}

		if err := fe.Extract(s); err != nil {
			return TimeEpoch, false, parseError(err, "time", s)
		}
	}
	res := fe.MakeTimeWithoutTimezone()
	return res, fe.currentTimeUsed, nil
}

// ParseTimestamp converts a string into a timestamp.
//
// The dependsOnContext return value indicates if we had to consult the given
// `now` value (either for the time or the local timezone).
func ParseTimestamp(
	now time.Time, dateStyle DateStyle, s string,
) (_ time.Time, dependsOnContext bool, _ error) {
	fe := fieldExtract{
		dateStyle:   dateStyle,
		currentTime: now,
		// A timestamp only actually needs a date component; the time
		// would be midnight.
		required: dateRequiredFields,
		wanted:   dateTimeFields,
	}

	if err := fe.Extract(s); err != nil {
		return TimeEpoch, false, parseError(err, "timestamp", s)
	}
	res := fe.MakeTimestamp()
	return res, fe.currentTimeUsed, nil
}

// ParseTimestampWithoutTimezone converts a string into a timestamp, stripping
// away any timezone information. Any specified timezone is inconsequential. The
// returned time always has UTC location.
//
// For example, all these inputs return 2020-06-26 01:02:03 +0000 UTC:
//   - '2020-06-26 01:02:03';
//   - '2020-06-26 01:02:03+04';
//   - 'now', if the local time (in the current timezone) is
//     2020-06-26 01:02:03. Note that this does not represent the same time
//     instant, but the one that "reads" the same in UTC.
//
// The dependsOnContext return value indicates if we had to consult the given
// `now` value (either for the time or the local timezone).
func ParseTimestampWithoutTimezone(
	now time.Time, dateStyle DateStyle, s string,
) (_ time.Time, dependsOnContext bool, _ error) {
	fe := fieldExtract{
		dateStyle:   dateStyle,
		currentTime: now,
		// A timestamp only actually needs a date component; the time
		// would be midnight.
		required: dateRequiredFields,
		wanted:   dateTimeFields,
	}

	if err := fe.Extract(s); err != nil {
		return TimeEpoch, false, parseError(err, "timestamp", s)
	}
	res := fe.MakeTimestampWithoutTimezone()
	return res, fe.currentTimeUsed, nil
}

// badFieldPrefixError constructs an error with pg code InvalidDatetimeFormat.
func badFieldPrefixError(field field, prefix rune) error {
	return inputErrorf("unexpected separator '%s' for field %s",
		string(prefix), errors.Safe(field.Pretty()))
}

// inputErrorf returns an error with pg code InvalidDatetimeFormat.
func inputErrorf(format string, args ...interface{}) error {
	err := errors.Newf(format, args...)
	return pgerror.WithCandidateCode(err, pgcode.InvalidDatetimeFormat)
}

// outOfRangeError returns an error with pg code DatetimeFieldOverflow.
func outOfRangeError(field string, val int) error {
	err := errors.Newf("field %s value %d is out of range", errors.Safe(field), errors.Safe(val))
	return errors.WithHint(
		pgerror.WithCandidateCode(err, pgcode.DatetimeFieldOverflow),
		`Perhaps you need a different "datestyle" setting.`,
	)
}

// parseError ensures that any error we return to the client will
// be some kind of error with a pg code.
func parseError(err error, kind string, s string) error {
	return pgerror.WithCandidateCode(
		errors.Wrapf(err, "parsing as type %s", errors.Safe(kind)),
		pgcode.InvalidDatetimeFormat)
}

// DefaultDateStyle returns the default datestyle for Postgres.
func DefaultDateStyle() DateStyle {
	return DateStyle{
		Order: Order_MDY,
		Style: Style_ISO,
	}
}

// ParseDateStyle parses a given DateStyle, modifying the existingDateStyle
// as appropriate. This is because specifying just Style or Order will leave
// the other field unchanged.
func ParseDateStyle(s string, existingDateStyle DateStyle) (DateStyle, error) {
	ds := existingDateStyle
	fields := strings.Split(s, ",")
	for _, field := range fields {
		field = strings.ToLower(strings.TrimSpace(field))
		switch field {
		case "iso":
			ds.Style = Style_ISO
		case "german":
			ds.Style = Style_GERMAN
		case "sql":
			ds.Style = Style_SQL
		case "postgres":
			ds.Style = Style_POSTGRES
		case "ymd":
			ds.Order = Order_YMD
		case "mdy":
			ds.Order = Order_MDY
		case "dmy":
			ds.Order = Order_DMY
		default:
			return ds, pgerror.Newf(pgcode.InvalidParameterValue, "unknown DateStyle parameter: %s", field)
		}
	}
	return ds, nil
}
