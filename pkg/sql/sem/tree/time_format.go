// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
)

// This block contains all available PG time formats.
const (
	PGTimeFormat              = "15:04:05.999999"
	PGDateFormat              = "2006-01-02"
	PGTimeStampFormatNoOffset = PGDateFormat + " " + PGTimeFormat
	PGTimeStampFormat         = PGTimeStampFormatNoOffset + "-07"
	PGTime2400Format          = "24:00:00"
	PGTimeTZFormat            = PGTimeFormat + "-07"
)

// WireFormatTime formats t into a format lib/pq understands, appending to the
// provided tmp buffer and reallocating if needed. The function will then return
// the resulting buffer.
// TODO(#sql-sessions): merge implementation with DTime.Format.
func WireFormatTime(t timeofday.TimeOfDay, tmp []byte) []byte {
	// time.Time's AppendFormat does not recognize 2400, so special case it accordingly.
	if t == timeofday.Time2400 {
		return []byte(PGTime2400Format)
	}
	return t.ToTime().AppendFormat(tmp, PGTimeFormat)
}

// WireFormatTimeTZ formats t into a format lib/pq understands, appending to the
// provided tmp buffer and reallocating if needed. The function will then return
// the resulting buffer.
// TODO(#sql-sessions): merge implementation with DTimeTZ.Format.
func WireFormatTimeTZ(t timetz.TimeTZ, tmp []byte) []byte {
	format := PGTimeTZFormat
	if t.OffsetSecs%60 != 0 {
		format += ":00:00"
	} else if t.OffsetSecs%3600 != 0 {
		format += ":00"
	}
	ret := t.ToTime().AppendFormat(tmp, format)
	// time.Time's AppendFormat does not recognize 2400, so special case it accordingly.
	if t.TimeOfDay == timeofday.Time2400 {
		// It instead reads 00:00:00. Replace that text.
		var newRet []byte
		newRet = append(newRet, PGTime2400Format...)
		newRet = append(newRet, ret[len(PGTime2400Format):]...)
		ret = newRet
	}
	return ret
}

// WireFormatTimestamp formats t into a format lib/pq understands.
// TODO(#sql-sessions): merge implementation with DTimestamp.Format.
func WireFormatTimestamp(t time.Time, offset *time.Location, tmp []byte) (b []byte) {
	var format string
	if offset != nil {
		format = PGTimeStampFormat
		if _, offsetSeconds := t.In(offset).Zone(); offsetSeconds%60 != 0 {
			format += ":00:00"
		} else if offsetSeconds%3600 != 0 {
			format += ":00"
		}
	} else {
		format = PGTimeStampFormatNoOffset
	}
	return WireFormatTimestampWithFormat(format, t, offset, tmp)
}

// WireFormatTimestampWithFormat formats t with an optional offset into a format
// lib/pq understands, appending to the provided tmp buffer and
// reallocating if needed. The function will then return the resulting
// buffer. WireFormatTimestampWithFormat is mostly cribbed from github.com/lib/pq.
// TODO(#sql-sessions): merge implementation with DTimestampTZ.Format.
func WireFormatTimestampWithFormat(
	format string, t time.Time, offset *time.Location, tmp []byte,
) (b []byte) {
	// Need to send dates before 0001 A.D. with " BC" suffix, instead of the
	// minus sign preferred by Go.
	// Beware, "0000" in ISO is "1 BC", "-0001" is "2 BC" and so on
	if offset != nil {
		t = t.In(offset)
	}

	bc := false
	if t.Year() <= 0 {
		// flip year sign, and add 1, e.g: "0" will be "1", and "-10" will be "11"
		t = t.AddDate((-t.Year())*2+1, 0, 0)
		bc = true
	}

	b = t.AppendFormat(tmp, format)
	if bc {
		b = append(b, " BC"...)
	}
	return b
}
