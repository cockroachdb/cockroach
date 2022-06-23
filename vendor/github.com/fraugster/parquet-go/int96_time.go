package goparquet

import (
	"encoding/binary"
	"time"
)

const (
	jan011970 = 2440588
	secPerDay = 24 * 60 * 60
)

var (
	tsUnixEpoch = time.Unix(0, 0)
)

func timeToJD(t time.Time) (uint32, uint64) {
	days := t.Unix() / secPerDay
	nSecs := t.UnixNano() - (days * secPerDay * int64(time.Second))

	// unix time starts from Jan 1, 1970 AC, this day is 2440588 day after the Jan 1, 4713 BC
	return uint32(days) + jan011970, uint64(nSecs)
}

func jdToTime(jd uint32, nsec uint64) time.Time {
	sec := int64(jd-jan011970) * secPerDay
	return time.Unix(sec, int64(nsec))
}

// Int96ToTime is a utility function to convert a Int96 Julian Date timestamp (https://en.wikipedia.org/wiki/Julian_day) to a time.Time.
// Please be aware that this function is limited to timestamps after the Unix epoch (Jan 01 1970 00:00:00 UTC) and cannot
// convert timestamps before that. The returned time does not contain a monotonic clock reading and is in the machine's current time zone.
func Int96ToTime(parquetDate [12]byte) time.Time {
	nano := binary.LittleEndian.Uint64(parquetDate[:8])
	dt := binary.LittleEndian.Uint32(parquetDate[8:])

	return jdToTime(dt, nano)
}

// TimeToInt96 is a utility function to convert a time.Time to an Int96 Julian Date timestamp (https://en.wikipedia.org/wiki/Julian_day).
// Please be aware that this function is limited to timestamps after the Unix epoch (Jan 01 1970 00:00:00 UTC) and cannot convert
// timestamps before that.
func TimeToInt96(t time.Time) [12]byte {
	var parquetDate [12]byte
	days, nSecs := timeToJD(t)
	binary.LittleEndian.PutUint64(parquetDate[:8], nSecs)
	binary.LittleEndian.PutUint32(parquetDate[8:], days)

	return parquetDate
}

// IsAfterUnixEpoch tests if a timestamp can be converted into Julian Day format, i.e. is it greater than 01-01-1970?
// Timestamps before this when converted will be corrupted when read back.
func IsAfterUnixEpoch(t time.Time) bool {
	return t.After(tsUnixEpoch)
}
