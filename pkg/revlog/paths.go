// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Object-path layout, mirroring revlog-format.md §3:
//
//	log/
//	  data/<tick-end>/<file_id>.sst
//	  resolved/<tick-end>.pb
//
// A tick-end is the path-fragment "YYYY-MM-DD/HH-MM.SS" (UTC). The
// embedded "/" makes the day half a real directory, so a reader can
// LIST log/resolved/ with delim="/" to enumerate days that have any
// closed tick, then LIST log/resolved/<day>/ flat to enumerate the
// ticks within that day.
const (
	logRoot     = "log"
	dataDir     = logRoot + "/data"
	resolvedDir = logRoot + "/resolved"
	sstExt      = ".sst"
	markerExt   = ".pb"

	tickEndLayout = "2006-01-02/15-04.05"
)

// ResolvedRoot is the LIST root for tick discovery.
const ResolvedRoot = resolvedDir + "/"

// FormatTickEnd renders a tick-end timestamp as the path-fragment
// "YYYY-MM-DD/HH-MM.SS" in UTC. Only the wall-clock component
// participates: tick boundaries are constructed with logical=0 and
// FormatTickEnd does not enforce that.
func FormatTickEnd(ts hlc.Timestamp) string {
	return time.Unix(0, ts.WallTime).UTC().Format(tickEndLayout)
}

// ParseTickEnd parses a tick-end fragment (as produced by
// FormatTickEnd) into an hlc.Timestamp with logical=0.
func ParseTickEnd(s string) (hlc.Timestamp, error) {
	t, err := time.ParseInLocation(tickEndLayout, s, time.UTC)
	if err != nil {
		return hlc.Timestamp{}, errors.Wrapf(err, "parsing tick-end %q", s)
	}
	return hlc.Timestamp{WallTime: t.UnixNano()}, nil
}

// MarkerPath is the path of one tick's close marker.
func MarkerPath(tickEnd hlc.Timestamp) string {
	return ResolvedRoot + FormatTickEnd(tickEnd) + markerExt
}

// DataDirPath is the parent directory of one tick's data files.
func DataDirPath(tickEnd hlc.Timestamp) string {
	return dataDir + "/" + FormatTickEnd(tickEnd) + "/"
}

// DataFilePath is the path of one data file in a tick.
func DataFilePath(tickEnd hlc.Timestamp, fileID int64) string {
	return DataDirPath(tickEnd) + strconv.FormatInt(fileID, 10) + sstExt
}
