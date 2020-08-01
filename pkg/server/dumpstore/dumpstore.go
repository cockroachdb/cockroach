// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package dumpstore

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// DumpStore represents a store for dump files.
//
// It must be coupled to a Dumper (see definition below) with the
// following contract: the lexical order of file names must also be
// the chronological order of their creation: this code is intended to
// track file order using file names. This choice was made both to
// avoid a mandatory sorting step after ioutil.ReadDir() and to
// present unsurprising behavior if the filesystem doesn't preserve
// timestamps properly (e.g. if a directory is restored from backup
// without timestamps).
//
// Its GC policy is to remove old files beyond the configured max size
// setting.
type DumpStore struct {
	dir                        string
	maxCombinedFileSizeSetting *settings.ByteSizeSetting
	st                         *cluster.Settings
}

// Dumper represents a dump file producer.
type Dumper interface {
	// PreFilter is called upon GC. The dumper may apply the cleanupFn
	// to young files it does not want to keep, even if they fit under
	// the configured max size.
	// The dumper may signal files that it wants to keep even
	// beyond the configured max size, by returning a boolean array
	// mapping 1-to-1 to the provided files.
	PreFilter(ctx context.Context, files []os.FileInfo, cleanupFn func(fileName string) error) (preserved map[int]bool, err error)

	// CheckOwnsFile returns nil if the dumper owns the given
	// file, an error otherwise.
	CheckOwnsFile(ctx context.Context, fi os.FileInfo) error
}

// NewStore creates a new DumpStore.
func NewStore(
	storeDir string, maxCombinedFileSizeSetting *settings.ByteSizeSetting, st *cluster.Settings,
) *DumpStore {
	return &DumpStore{
		dir:                        storeDir,
		maxCombinedFileSizeSetting: maxCombinedFileSizeSetting,
		st:                         st,
	}
}

// GetFullPath retrieves the full path for a file under this store.
func (s *DumpStore) GetFullPath(fileName string) string {
	return filepath.Join(s.dir, fileName)
}

// GC runs the GC policy on this store.
func (s *DumpStore) GC(ctx context.Context, now time.Time, dumper Dumper) {
	// NB: ioutil.ReadDir sorts the file names in ascending order.
	// This brings the oldest files first.
	files, err := ioutil.ReadDir(s.dir)
	if err != nil {
		log.Warningf(ctx, "%v", err)
		return
	}

	maxS := s.maxCombinedFileSizeSetting.Get(&s.st.SV)

	cleanupFn := func(fileName string) error {
		path := filepath.Join(s.dir, fileName)
		return os.Remove(path)
	}

	// Determine which files the dumper really wants
	// to keep.
	preserved, err := dumper.PreFilter(ctx, files, cleanupFn)
	if err != nil {
		log.Warningf(ctx, "%v", err)
		return
	}

	// Then, remove all the oldest files whose size goes in excess of
	// maxS, except for those listed in preserved.
	removeOldAndTooBigExcept(ctx, dumper, files, now, maxS, preserved, cleanupFn)
}

// removeOldAndTooBigExcept looks at the entries in files and calls
// the fn closure for every entry not in the preserved map whose size
// causes the combined collection of files to exceed maxS.
//
// Files that don't match the output file prefix are ignored
// (i.e. preserved).
func removeOldAndTooBigExcept(
	ctx context.Context,
	dumper Dumper,
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
		if err := dumper.CheckOwnsFile(ctx, fi); err != nil {
			log.Infof(ctx, "ignoring unknown file %s: %v", fi.Name(), err)
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
				log.Warningf(ctx, "cannot remove file %s: %v", fi.Name(), err)
			}
		}
	}
}
