// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package heapprofiler

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var (
	maxProfiles = settings.RegisterIntSetting(
		"server.heap_profile.max_profiles",
		"maximum number of profiles to be kept per ramp-up of memory usage. "+
			"A ramp-up is defined as a sequence of profiles with increasing heap usage.",
		5,
	)

	maxCombinedFileSize = settings.RegisterByteSizeSetting(
		"server.heap_profile.total_dump_size_limit",
		"maximum combined disk size of preserved profiles",
		128<<20, // 128MiB
	)
)

// profileStore represents the directory where heap profiles are stored.
// It supports automatic garbage collection of old profiles.
type profileStore struct {
	dir string
	st  *cluster.Settings
}

const fileNamePrefix = "memprof"

// timestampFormat is chosen to mimix that used by the log
// package. This is not a hard requirement thought; the profiles are
// stored in a separate directory.
const timestampFormat = "2006-01-02T15_04_05.999"

func (s *profileStore) makeNewFileName(timestamp time.Time, curHeap uint64) string {
	// We place the timestamp immediately after the (immutable) file
	// prefix to ensure that a directory listing sort also sorts the
	// profiles in timestamp order.
	fileName := fmt.Sprintf("%s.%s.%d",
		fileNamePrefix, timestamp.Format(timestampFormat), curHeap)
	return filepath.Join(s.dir, fileName)
}

func (s *profileStore) gcProfiles(ctx context.Context, now time.Time) {
	// NB: ioutil.ReadDir sorts the file names in ascending order.
	// This brings the oldest files first.
	files, err := ioutil.ReadDir(s.dir)
	if err != nil {
		log.Warningf(ctx, "%v", err)
		return
	}
	maxP := maxProfiles.Get(&s.st.SV)
	maxS := maxCombinedFileSize.Get(&s.st.SV)

	cleanupFn := func(fileName string) error {
		path := filepath.Join(s.dir, fileName)
		return os.Remove(path)
	}

	// First, detect the "last ramp-up" and remove all but the last
	// maxProfiles profile dumps.
	preserved := cleanupLastRampup(ctx, files, maxP, cleanupFn)

	// Then, remove all the oldest files whose size goes in excess of
	// maxS, except for those listed in preserved.
	removeOldAndTooBigExcept(ctx, files, now, maxS, preserved, cleanupFn)
}

// removeOldAndTooBigExcept looks at the entries in files and calls
// the fn closure for every entry not in the preserved map whose size
// causes the combined collection of files to exceed maxS.
func removeOldAndTooBigExcept(
	ctx context.Context,
	files []os.FileInfo,
	now time.Time,
	maxS int64,
	preserved map[int]bool,
	fn func(string) error,
) {
	actualSize := int64(0)
	// Go from newest to latest: we want to prioritize keeping files that are newer.
	for i := len(files) - 1; i >= 0; i-- {
		fi := files[i]

		// Ignore files from another generator.
		if !strings.HasPrefix(fi.Name(), fileNamePrefix) {
			log.Infof(ctx, "ignoring unknown heap profile %s", fi.Name())
			continue
		}

		// Note: we are counting preserved files against the maximum.
		actualSize += fi.Size()

		// Ignore all the preserved entries, even if they are "too old".
		if preserved[i] {
			continue
		}

		// Does the entry make the directory grow past the maximum?
		if actualSize > maxS {
			// Yes: pass it to the closure.
			if err := fn(fi.Name()); err != nil {
				log.Warningf(ctx, "cannot remove heap prof file %s: %v", fi.Name(), err)
			}
		}
	}
}

// cleanupLastRampup parses the filenames in files to detect the
// last ramp-up (sequence of increasing heap usage). If there
// are more than maxD entries in the last ramp-up, the fn closure
// is called for each of them.
//
// files is assumed to be sorted in chronological order already,
// oldest entry first.
//
// The preserved return value contains the indexes in files
// corresponding to the last ramp-up that were not passed to fn.
func cleanupLastRampup(
	ctx context.Context, files []os.FileInfo, maxP int64, fn func(string) error,
) (preserved map[int]bool) {
	preserved = make(map[int]bool)
	curMaxHeap := uint64(math.MaxUint64)
	numFiles := int64(0)
	for i := len(files) - 1; i >= 0; i-- {
		_, curHeap, err := parseFileName(files[i].Name())
		if err != nil {
			log.Warningf(ctx, "%v", err)
			continue
		}

		if curHeap > curMaxHeap {
			// This is the end of a ramp-up sequence. We're done.
			break
		}

		// Keep the currently seen heap for the next iteration.
		curMaxHeap = curHeap

		// We saw one file.
		numFiles++

		// Did we encounter the maximum?
		if numFiles > maxP {
			// Yes: clean this up.
			if err := fn(files[i].Name()); err != nil {
				log.Warningf(ctx, "%v", err)
			}
		} else {
			// No: we preserve this file.
			preserved[i] = true
		}
	}

	return preserved
}

// parseFileName retrieves the components of a file name generated by makeNewFileName().
func parseFileName(fileName string) (timestamp time.Time, heapUsage uint64, err error) {
	parts := strings.Split(fileName, ".")
	const numParts = 4 /* prefix, date/time, milliseconds, heap usage */
	if len(parts) != numParts || parts[0] != fileNamePrefix {
		return timestamp, heapUsage, errors.Newf("filename does not look like a profile dump: %s", fileName)
	}
	maybeTimestamp := parts[1] + "." + parts[2]
	timestamp, err = time.Parse(timestampFormat, maybeTimestamp)
	if err != nil {
		return timestamp, heapUsage, errors.Wrapf(err, "%s", fileName)
	}
	heapUsage, err = strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		err = errors.Wrapf(err, "%s", fileName)
	}
	return
}
