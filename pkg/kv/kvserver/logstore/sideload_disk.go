// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/time/rate"
)

var _ SideloadStorage = &DiskSideloadStorage{}

// DiskSideloadStorage implements SideloadStorage using the given storage
// engine.
//
// TODO(pavelkalinnikov): remove the interface, this type is the only impl.
type DiskSideloadStorage struct {
	st      *cluster.Settings
	limiter *rate.Limiter
	dir     string
	eng     storage.Engine
}

func sideloadedPath(baseDir string, rangeID roachpb.RangeID) string {
	// Use one level of sharding to avoid too many items per directory. For
	// example, ext3 and older ext4 support only 32k and 64k subdirectories
	// per directory, respectively. Newer FS typically have no such limitation,
	// but still.
	//
	// For example, r1828 will end up in baseDir/sideloading/r1XXX/r1828.
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

// Dir implements SideloadStorage.
func (ss *DiskSideloadStorage) Dir() string {
	return ss.dir
}

// Put implements SideloadStorage.
func (ss *DiskSideloadStorage) Put(
	ctx context.Context, index kvpb.RaftIndex, term kvpb.RaftTerm, contents []byte,
) error {
	filename := ss.filename(ctx, index, term)
	// There's a chance the whole path is missing (for example after Clear()),
	// in which case handle that transparently.
	for {
		// Use 0644 since that's what RocksDB uses:
		// https://github.com/facebook/rocksdb/blob/56656e12d67d8a63f1e4c4214da9feeec2bd442b/env/env_posix.cc#L171
		if err := kvserverbase.WriteFileSyncing(ctx, filename, contents, ss.eng.Env(), 0644, ss.st, ss.limiter, fs.PebbleIngestionWriteCategory); err == nil {
			return nil
		} else if !oserror.IsNotExist(err) {
			return err
		}
		// Ensure that ss.dir exists. The filename() is placed directly in ss.dir,
		// so the next loop iteration should succeed.
		if err := mkdirAllAndSyncParents(ss.eng.Env(), ss.dir, os.ModePerm); err != nil {
			return err
		}
		continue
	}
}

// Sync implements SideloadStorage.
func (ss *DiskSideloadStorage) Sync() error {
	dir, err := ss.eng.Env().OpenDir(ss.dir)
	// The directory can be missing because we did not Put() any entry to it yet,
	// or it has been removed by TruncateTo() or Clear().
	//
	// TODO(pavelkalinnikov): if ss.dir existed and has been removed, we should
	// sync the parent of ss.dir, to persist the removal. Otherwise it may come
	// back after a restart. Alternatively, and more likely, we should cleanup
	// leftovers upon restart - we have other TODOs for that.
	if oserror.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}

// Get implements SideloadStorage.
func (ss *DiskSideloadStorage) Get(
	ctx context.Context, index kvpb.RaftIndex, term kvpb.RaftTerm,
) ([]byte, error) {
	filename := ss.filename(ctx, index, term)
	b, err := fs.ReadFile(ss.eng.Env(), filename)
	if oserror.IsNotExist(err) {
		return nil, errSideloadedFileNotFound
	}
	return b, err
}

// Filename implements SideloadStorage.
func (ss *DiskSideloadStorage) Filename(
	ctx context.Context, index kvpb.RaftIndex, term kvpb.RaftTerm,
) (string, error) {
	return ss.filename(ctx, index, term), nil
}

func (ss *DiskSideloadStorage) filename(
	ctx context.Context, index kvpb.RaftIndex, term kvpb.RaftTerm,
) string {
	return filepath.Join(ss.dir, fmt.Sprintf("i%d.t%d", index, term))
}

// Purge implements SideloadStorage.
func (ss *DiskSideloadStorage) Purge(
	ctx context.Context, index kvpb.RaftIndex, term kvpb.RaftTerm,
) (int64, error) {
	return ss.purgeFile(ctx, ss.filename(ctx, index, term))
}

func (ss *DiskSideloadStorage) fileSize(filename string) (int64, error) {
	info, err := ss.eng.Env().Stat(filename)
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
	if err := ss.eng.Env().Remove(filename); err != nil {
		if oserror.IsNotExist(err) {
			return 0, errSideloadedFileNotFound
		}
		return 0, err
	}
	return size, nil
}

// Clear implements SideloadStorage.
func (ss *DiskSideloadStorage) Clear(_ context.Context) error {
	return ss.eng.Env().RemoveAll(ss.dir)
}

// TruncateTo implements SideloadStorage.
func (ss *DiskSideloadStorage) TruncateTo(
	ctx context.Context, firstIndex kvpb.RaftIndex,
) (bytesFreed, bytesRetained int64, _ error) {
	return ss.possiblyTruncateTo(ctx, 0, firstIndex, true /* doTruncate */)
}

// Helper for truncation or byte calculation for [from, to).
func (ss *DiskSideloadStorage) possiblyTruncateTo(
	ctx context.Context, from kvpb.RaftIndex, to kvpb.RaftIndex, doTruncate bool,
) (bytesFreed, bytesRetained int64, _ error) {
	deletedAll := true
	if err := ss.forEach(ctx, func(index kvpb.RaftIndex, filename string) (bool, error) {
		if index >= to {
			size, err := ss.fileSize(filename)
			if err != nil {
				return false, err
			}
			bytesRetained += size
			deletedAll = false
			return true, nil
		}
		if index < from {
			// TODO(pavelkalinnikov): these files may never be removed. Clean them up.
			return true, nil
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
			return false, err
		}
		bytesFreed += fileSize
		return true, nil
	}); err != nil {
		return 0, 0, err
	}

	if deletedAll && doTruncate {
		// The directory may not exist, or it may exist and have been empty.
		// Not worth trying to figure out which one, just try to delete.
		err := ss.eng.Env().Remove(ss.dir)
		if err != nil && !oserror.IsNotExist(err) {
			// TODO(pavelkalinnikov): this is possible because deletedAll can be left
			// true despite existence of files with index < from which are skipped.
			log.Infof(ctx, "unable to remove sideloaded dir %s: %v", ss.dir, err)
			err = nil // handled
		}
	}
	return bytesFreed, bytesRetained, nil
}

// HasAnyEntry implements SideloadStorage.
func (ss *DiskSideloadStorage) HasAnyEntry(
	ctx context.Context, from, to kvpb.RaftIndex,
) (bool, error) {
	// Find any file at index in [from, to).
	found := false
	if err := ss.forEach(ctx, func(index kvpb.RaftIndex, _ string) (bool, error) {
		if index >= from && index < to {
			found = true
			return false, nil // stop the iteration
		}
		return true, nil
	}); err != nil {
		return false, err
	}
	return found, nil
}

// BytesIfTruncatedFromTo implements SideloadStorage.
func (ss *DiskSideloadStorage) BytesIfTruncatedFromTo(
	ctx context.Context, from kvpb.RaftIndex, to kvpb.RaftIndex,
) (freed, retained int64, _ error) {
	return ss.possiblyTruncateTo(ctx, from, to, false /* doTruncate */)
}

// forEach runs the given visit function for each file in the sideloaded storage
// directory. If visit returns false, forEach terminates early and returns nil.
// If visit returns an error, forEach terminates early and returns an error.
func (ss *DiskSideloadStorage) forEach(
	ctx context.Context, visit func(index kvpb.RaftIndex, filename string) (bool, error),
) error {
	// TODO(pavelkalinnikov): consider making the List method iterative.
	matches, err := ss.eng.Env().List(ss.dir)
	if oserror.IsNotExist(err) {
		return nil // nothing to do
	} else if err != nil {
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
		if keepGoing, err := visit(kvpb.RaftIndex(logIdx), match); err != nil {
			return errors.Wrapf(err, "matching pattern %q on dir %s", match, ss.dir)
		} else if !keepGoing {
			return nil
		}
	}
	return nil
}

// String lists the files in the storage without guaranteeing an ordering.
func (ss *DiskSideloadStorage) String() string {
	var buf strings.Builder
	var count int
	if err := ss.forEach(context.Background(), func(_ kvpb.RaftIndex, filename string) (bool, error) {
		count++
		_, _ = fmt.Fprintln(&buf, filename)
		return true, nil
	}); err != nil {
		return err.Error()
	}
	fmt.Fprintf(&buf, "(%d files)\n", count)
	return buf.String()
}

// mkdirAllAndSyncParents creates the given directory and all its missing
// parents if any. For every newly created directly, it syncs the corresponding
// parent directory. The directories are created using the provided permissions
// mask, with the same semantics as in os.MkdirAll.
//
// For example, if path is "/x/y/z", and "/x" previously existed, then this func
// creates "/x/y" and "/x/y/z", and syncs directories "/x" and "/x/y".
//
// TODO(pavelkalinnikov): this does not work well with paths containing . and ..
// elements inside the data-dir directory. We don't construct the path this way
// though, right now any non-canonical part of the path would be only in the
// <data-dir> path.
//
// TODO(pavelkalinnikov): have a type-safe canonical path type which can be
// iterated without thinking about . and .. placeholders.
func mkdirAllAndSyncParents(fs vfs.FS, path string, perm os.FileMode) error {
	// Find the lowest existing directory in the hierarchy.
	var exists string
	for dir, parent := path, ""; ; dir = parent {
		if _, err := fs.Stat(dir); err == nil {
			exists = dir
			break
		} else if !oserror.IsNotExist(err) {
			return errors.Wrapf(err, "could not get dir info: %s", dir)
		}
		parent = fs.PathDir(dir)
		// NB: not checking against the separator, to be platform-agnostic.
		if dir == "." || parent == dir { // reached the topmost dir or the root
			return errors.Newf("topmost dir does not exist: %s", dir)
		}
	}

	// Create the destination directory and any of its missing parents.
	if err := fs.MkdirAll(path, perm); err != nil {
		return errors.Wrapf(err, "could not create all directories: %s", path)
	}

	// Sync parent directories up to the lowest existing ancestor, included.
	for dir, parent := path, ""; dir != exists; dir = parent {
		parent = fs.PathDir(dir)
		if handle, err := fs.OpenDir(parent); err != nil {
			return errors.Wrapf(err, "could not open parent dir: %s", parent)
		} else if err := handle.Sync(); err != nil {
			_ = handle.Close()
			return errors.Wrapf(err, "could not sync parent dir: %s", parent)
		} else if err := handle.Close(); err != nil {
			return errors.Wrapf(err, "could not close parent dir: %s", parent)
		}
	}

	return nil
}
