// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logstore

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"golang.org/x/time/rate"
)

var _ SideloadStorage = &DiskSideloadStorage{}

// DiskSideloadStorage implements SideloadStorage using the given storage
// engine.
//
// TODO(pavelkalinnikov): remove the interface, this type is the only impl.
type DiskSideloadStorage struct {
	st         *cluster.Settings
	limiter    *rate.Limiter
	dir        string
	dirCreated bool
	eng        storage.Engine
}

func sideloadedPath(baseDir string, rangeID roachpb.RangeID) string {
	// Use one level of sharding to avoid too many items per directory. For
	// example, ext3 and older ext4 support only 32k and 64k subdirectories
	// per directory, respectively. Newer FS typically have no such limitation,
	// but still.
	//
	// For example, r1828 will end up in baseDir/r1XXX/r1828.
	return filepath.Join(
		baseDir,
		"sideloading",
		fmt.Sprintf("r%dXXXX", rangeID/10000), // sharding
		fmt.Sprintf("r%d", rangeID),
	)
}

// NewDiskSideloadStorage creates a SideloadStorage for a given replica, stored
// in the specified engine.
func NewDiskSideloadStorage(
	st *cluster.Settings,
	rangeID roachpb.RangeID,
	baseDir string,
	limiter *rate.Limiter,
	eng storage.Engine,
) *DiskSideloadStorage {
	return &DiskSideloadStorage{
		dir:     sideloadedPath(baseDir, rangeID),
		eng:     eng,
		st:      st,
		limiter: limiter,
	}
}

func (ss *DiskSideloadStorage) createDir() error {
	err := ss.eng.MkdirAll(ss.dir)
	ss.dirCreated = ss.dirCreated || err == nil
	return err
}

// Dir implements SideloadStorage.
func (ss *DiskSideloadStorage) Dir() string {
	return ss.dir
}

// Put implements SideloadStorage.
func (ss *DiskSideloadStorage) Put(ctx context.Context, index, term uint64, contents []byte) error {
	filename := ss.filename(ctx, index, term)
	// There's a chance the whole path is missing (for example after Clear()),
	// in which case handle that transparently.
	for {
		// Use 0644 since that's what RocksDB uses:
		// https://github.com/facebook/rocksdb/blob/56656e12d67d8a63f1e4c4214da9feeec2bd442b/env/env_posix.cc#L171
		if err := kvserverbase.WriteFileSyncing(ctx, filename, contents, ss.eng, 0644, ss.st, ss.limiter); err == nil {
			return nil
		} else if !oserror.IsNotExist(err) {
			return err
		}
		// createDir() ensures ss.dir exists but will not create any subdirectories
		// within ss.dir because filename() does not make subdirectories in ss.dir.
		if err := ss.createDir(); err != nil {
			return err
		}
		continue
	}
}

// Get implements SideloadStorage.
func (ss *DiskSideloadStorage) Get(ctx context.Context, index, term uint64) ([]byte, error) {
	filename := ss.filename(ctx, index, term)
	b, err := fs.ReadFile(ss.eng, filename)
	if oserror.IsNotExist(err) {
		return nil, errSideloadedFileNotFound
	}
	return b, err
}

// Filename implements SideloadStorage.
func (ss *DiskSideloadStorage) Filename(ctx context.Context, index, term uint64) (string, error) {
	return ss.filename(ctx, index, term), nil
}

func (ss *DiskSideloadStorage) filename(ctx context.Context, index, term uint64) string {
	return filepath.Join(ss.dir, fmt.Sprintf("i%d.t%d", index, term))
}

// Purge implements SideloadStorage.
func (ss *DiskSideloadStorage) Purge(ctx context.Context, index, term uint64) (int64, error) {
	return ss.purgeFile(ctx, ss.filename(ctx, index, term))
}

func (ss *DiskSideloadStorage) fileSize(filename string) (int64, error) {
	info, err := ss.eng.Stat(filename)
	if err != nil {
		if oserror.IsNotExist(err) {
			return 0, errSideloadedFileNotFound
		}
		return 0, err
	}
	return info.Size(), nil
}

func (ss *DiskSideloadStorage) purgeFile(ctx context.Context, filename string) (int64, error) {
	size, err := ss.fileSize(filename)
	if err != nil {
		return 0, err
	}
	if err := ss.eng.Remove(filename); err != nil {
		if oserror.IsNotExist(err) {
			return 0, errSideloadedFileNotFound
		}
		return 0, err
	}
	return size, nil
}

// Clear implements SideloadStorage.
func (ss *DiskSideloadStorage) Clear(_ context.Context) error {
	err := ss.eng.RemoveAll(ss.dir)
	ss.dirCreated = ss.dirCreated && err != nil
	return err
}

// TruncateTo implements SideloadStorage.
func (ss *DiskSideloadStorage) TruncateTo(
	ctx context.Context, firstIndex uint64,
) (bytesFreed, bytesRetained int64, _ error) {
	return ss.possiblyTruncateTo(ctx, 0, firstIndex, true /* doTruncate */)
}

// Helper for truncation or byte calculation for [from, to).
func (ss *DiskSideloadStorage) possiblyTruncateTo(
	ctx context.Context, from uint64, to uint64, doTruncate bool,
) (bytesFreed, bytesRetained int64, _ error) {
	deletedAll := true
	if err := ss.forEach(ctx, func(index uint64, filename string) error {
		if index >= to {
			size, err := ss.fileSize(filename)
			if err != nil {
				return err
			}
			bytesRetained += size
			deletedAll = false
			return nil
		}
		if index < from {
			return nil
		}
		// index is in [from, to)
		var fileSize int64
		var err error
		if doTruncate {
			fileSize, err = ss.purgeFile(ctx, filename)
		} else {
			fileSize, err = ss.fileSize(filename)
		}
		if err != nil {
			return err
		}
		bytesFreed += fileSize
		return nil
	}); err != nil {
		return 0, 0, err
	}

	if deletedAll && doTruncate {
		// The directory may not exist, or it may exist and have been empty.
		// Not worth trying to figure out which one, just try to delete.
		err := ss.eng.Remove(ss.dir)
		if err != nil && !oserror.IsNotExist(err) {
			log.Infof(ctx, "unable to remove sideloaded dir %s: %v", ss.dir, err)
			err = nil // handled
		}
	}
	return bytesFreed, bytesRetained, nil
}

// BytesIfTruncatedFromTo implements SideloadStorage.
func (ss *DiskSideloadStorage) BytesIfTruncatedFromTo(
	ctx context.Context, from uint64, to uint64,
) (freed, retained int64, _ error) {
	return ss.possiblyTruncateTo(ctx, from, to, false /* doTruncate */)
}

func (ss *DiskSideloadStorage) forEach(
	ctx context.Context, visit func(index uint64, filename string) error,
) error {
	matches, err := ss.eng.List(ss.dir)
	if oserror.IsNotExist(err) {
		// Nothing to do.
		return nil
	}
	if err != nil {
		return err
	}
	for _, match := range matches {
		// List returns a relative path, but we want to deal in absolute paths
		// because we may pass this back to `eng.{Delete,Stat}`, etc, and those
		// expect absolute paths.
		match = filepath.Join(ss.dir, match)
		base := filepath.Base(match)
		// Extract `i<log-index>` prefix from file.
		if len(base) < 1 || base[0] != 'i' {
			continue
		}
		base = base[1:]
		upToDot := strings.SplitN(base, ".", 2)
		logIdx, err := strconv.ParseUint(upToDot[0], 10, 64)
		if err != nil {
			log.Infof(ctx, "unexpected file %s in sideloaded directory %s", match, ss.dir)
			continue
		}
		if err := visit(logIdx, match); err != nil {
			return errors.Wrapf(err, "matching pattern %q on dir %s", match, ss.dir)
		}
	}
	return nil
}

// String lists the files in the storage without guaranteeing an ordering.
func (ss *DiskSideloadStorage) String() string {
	var buf strings.Builder
	var count int
	if err := ss.forEach(context.Background(), func(_ uint64, filename string) error {
		count++
		_, _ = fmt.Fprintln(&buf, filename)
		return nil
	}); err != nil {
		return err.Error()
	}
	fmt.Fprintf(&buf, "(%d files)\n", count)
	return buf.String()
}
