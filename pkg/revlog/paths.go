// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"fmt"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Object-path layout:
//
//	log/
//	  data/<tick-end>/<file_id>.sst
//	  resolved/<tick-end>.pb
//	  coverage/<effective-from-HLC>
//
// A tick-end is the path-fragment "YYYY-MM-DD/HH-MM.SS" (UTC). The
// embedded "/" makes the day half a real directory, so a reader can
// LIST log/resolved/ with delim="/" to enumerate days that have any
// closed tick, then LIST log/resolved/<day>/ flat to enumerate the
// ticks within that day.
//
// Coverage HLCs are formatted as fixed-width
// "<19-digit-wall-nanos>_<10-digit-logical>" so a flat lex sort
// matches HLC ordering (see FormatHLCName / ParseHLCName).
const (
	logRoot     = "log"
	dataDir     = logRoot + "/data"
	resolvedDir = logRoot + "/resolved"
	coverageDir = logRoot + "/coverage"
	sstExt      = ".sst"
	markerExt   = ".pb"

	tickEndLayout = "2006-01-02/15-04.05"
)

// ResolvedRoot is the LIST root for tick discovery.
const ResolvedRoot = resolvedDir + "/"

// CoverageRoot is the LIST root for coverage-epoch discovery.
const CoverageRoot = coverageDir + "/"

// FormatTickEnd renders a tick-end timestamp as the path-fragment
// "YYYY-MM-DD/HH-MM.SS" in UTC. Only the wall-clock component
// participates: tick boundaries are constructed with logical=0 and
// FormatTickEnd does not enforce that.
func FormatTickEnd(ts hlc.Timestamp) string {
	return timeutil.Unix(0, ts.WallTime).UTC().Format(tickEndLayout)
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

// FormatHLCName renders an HLC as a fixed-width, lex-sortable
// "<19-digit-wall-nanos>_<10-digit-logical>" path-name fragment.
// 19 digits accommodates the int64 max wall-time (9223372036854775807,
// 19 digits); 10 digits accommodates the int32 max logical
// (2147483647, 10 digits). The fixed widths mean lex order on
// formatted names equals HLC order — required for the flat LIST +
// "largest entry with HLC <= T" lookup pattern used by the coverage
// and schema descs subtrees.
func FormatHLCName(ts hlc.Timestamp) string {
	return fmt.Sprintf("%019d_%010d", ts.WallTime, ts.Logical)
}

// ParseHLCName is the inverse of FormatHLCName.
func ParseHLCName(s string) (hlc.Timestamp, error) {
	var wall int64
	var logical int32
	if _, err := fmt.Sscanf(s, "%019d_%010d", &wall, &logical); err != nil {
		return hlc.Timestamp{}, errors.Wrapf(err, "parsing HLC name %q", s)
	}
	return hlc.Timestamp{WallTime: wall, Logical: logical}, nil
}

// CoveragePath is the path of one coverage epoch's object.
func CoveragePath(effectiveFrom hlc.Timestamp) string {
	return CoverageRoot + FormatHLCName(effectiveFrom)
}
