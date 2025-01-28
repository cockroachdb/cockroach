// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package disk

import (
	"bytes"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unsafe"

	"github.com/cockroachdb/errors"
)

// parseDiskStats parses disk stats from the provided byte slice of data read
// from /proc/diskstats. It takes a slice of *monitoredDisks sorted by device
// ID. Any devices not listed are ignored during parsing.
//
// Documentation of /proc/diskstats was retrieved from:
// https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats
//
//	The /proc/diskstats file displays the I/O statistics
//	of block devices. Each line contains the following 14
//	fields:
//
//	1  major number
//	2  minor number
//	3  device name
//	4  reads completed successfully
//	5  reads merged
//	6  sectors read
//	7  time spent reading (ms)
//	8  writes completed
//	9  writes merged
//	10  sectors written
//	11  time spent writing (ms)
//	12  I/Os currently in progress
//	13  time spent doing I/Os (ms)
//	14  weighted time spent doing I/Os (ms)
func parseDiskStats(contents []byte, disks []*monitoredDisk, measuredAt time.Time) error {
	for lineNum := 0; len(contents) > 0; lineNum++ {
		lineBytes, rest := splitLine(contents)
		line := unsafe.String(&lineBytes[0], len(lineBytes))
		contents = rest

		line = strings.TrimSpace(line)

		var deviceID DeviceID
		if devMajor, rest, err := mustParseUint(line, 32, "deviceID.major"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		} else {
			line = rest
			deviceID.major = uint32(devMajor)
		}
		if devMinor, rest, err := mustParseUint(line, 32, "deviceID.minor"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		} else {
			line = rest
			deviceID.minor = uint32(devMinor)
		}
		diskIdx := slices.IndexFunc(disks, func(d *monitoredDisk) bool {
			return d.deviceID == deviceID
		})
		if diskIdx == -1 {
			// This device doesn't exist in the list of devices being monitored,
			// so skip it.
			continue
		}

		var stats Stats
		var err error
		stats.DeviceName, line = splitFieldDelim(line)
		if stats.ReadsCount, line, err = mustParseUint(line, 64, "reads count"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		}
		if stats.ReadsMerged, line, err = mustParseUint(line, 64, "reads merged"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		}
		if stats.ReadsSectors, line, err = mustParseUint(line, 64, "reads sectors"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		}
		if millis, rest, err := mustParseUint(line, 64, "reads duration"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		} else {
			line = rest
			stats.ReadsDuration = time.Duration(millis) * time.Millisecond
		}
		if stats.WritesCount, line, err = mustParseUint(line, 64, "writes count"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		}
		if stats.WritesMerged, line, err = mustParseUint(line, 64, "writes merged"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		}
		if stats.WritesSectors, line, err = mustParseUint(line, 64, "writes sectors"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		}
		if millis, rest, err := mustParseUint(line, 64, "writes duration"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		} else {
			line = rest
			stats.WritesDuration = time.Duration(millis) * time.Millisecond
		}
		if stats.InProgressCount, line, err = mustParseUint(line, 64, "inprogress iops"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		}
		if millis, rest, err := mustParseUint(line, 64, "time doing IO"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		} else {
			line = rest
			stats.CumulativeDuration = time.Duration(millis) * time.Millisecond
		}
		if millis, rest, err := mustParseUint(line, 64, "weighted IO duration"); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		} else {
			line = rest
			stats.WeightedIODuration = time.Duration(millis) * time.Millisecond
		}

		// The below fields are optional.
		if stats.DiscardsCount, _, line, err = tryParseUint(line, 64); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		}
		if stats.DiscardsMerged, _, line, err = tryParseUint(line, 64); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		}
		if stats.DiscardsSectors, _, line, err = tryParseUint(line, 64); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		}
		if millis, ok, rest, err := tryParseUint(line, 64); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		} else if ok {
			line = rest
			stats.DiscardsDuration = time.Duration(millis) * time.Millisecond
		}
		if stats.FlushesCount, _, line, err = tryParseUint(line, 64); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		}
		if millis, ok, _, err := tryParseUint(line, 64); err != nil {
			return errors.Wrapf(err, "/proc/diskstats:%d: %q", lineNum, err)
		} else if ok {
			stats.FlushesDuration = time.Duration(millis) * time.Millisecond
		}
		disks[diskIdx].recordStats(measuredAt, stats)
	}
	return nil
}

func splitLine(b []byte) (line, rest []byte) {
	i := bytes.IndexByte(b, '\n')
	if i >= 0 {
		return b[:i], b[i+1:]
	}
	return b, nil
}

// splitFieldDelim accepts a string beginning with a non-whitespace character.
// It returns the prefix of the string of non-whitespace characters (`field`),
// and a slice that holds the beginning of the following string of
// non-whitespace characters.
func splitFieldDelim(s string) (field, next string) {
	var ok bool
	for i, r := range s {
		// Whitespace delimits fields.
		if unicode.IsSpace(r) {
			// If !ok, this is the first whitespace we've seen. Set field. We
			// don't stop iterating because we still need to find the start of
			// `next`.
			if !ok {
				ok = true
				field = s[:i]
			}
		} else if ok {
			// This is the first non-whitespace character after the delimiter.
			// We know this is where the following field begins and can return
			// now.
			next = s[i:]
			return field, next
		}
	}
	// We never found a delimiter, or the delimiter never ended.
	return field, next
}

func mustParseUint(s string, bitSize int, fieldName string) (v int, next string, err error) {
	var exists bool
	v, exists, next, err = tryParseUint(s, bitSize)
	if err != nil {
		return v, next, err
	} else if !exists {
		return 0, next, errors.Newf("%s field not present", errors.Safe(fieldName))
	}
	return v, next, nil
}

func tryParseUint(s string, bitSize int) (v int, ok bool, next string, err error) {
	var field string
	field, next = splitFieldDelim(s)
	if len(field) == 0 {
		return v, false, next, nil
	}
	if v, err := strconv.ParseUint(field, 10, bitSize); err != nil {
		return 0, true, next, err
	} else {
		return int(v), true, next, nil
	}
}
