// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"golang.org/x/time/rate"
)

var _ SideloadStorage = &diskSideloadStorage{}

type diskSideloadStorage struct {
	st         *cluster.Settings
	limiter    *rate.Limiter
	dir        string
	dirCreated bool
	eng        storage.Engine
}

func deprecatedSideloadedPath(
	baseDir string, rangeID roachpb.RangeID, replicaID roachpb.ReplicaID,
) string {
	return filepath.Join(
		baseDir,
		"sideloading",
		fmt.Sprintf("%d", rangeID%1000), // sharding
		fmt.Sprintf("%d.%d", rangeID, replicaID),
	)
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

func exists(eng storage.Engine, path string) (bool, error) {
	_, err := eng.Stat(path)
	if err == nil {
		return true, nil
	}
	if oserror.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func newDiskSideloadStorage(
	st *cluster.Settings,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	baseDir string,
	limiter *rate.Limiter,
	eng storage.Engine,
) (*diskSideloadStorage, error) {
	path := deprecatedSideloadedPath(baseDir, rangeID, replicaID)
	newPath := sideloadedPath(baseDir, rangeID)
	// NB: this call to exists() is in the hot path when the server starts
	// as it will be called once for each replica. However, during steady
	// state (i.e. when the version variable hasn't *just* flipped), we're
	// expecting `path` to not exist (since it refers to the legacy path at
	// the moment). A stat call for a directory that doesn't exist isn't
	// very expensive (on the order of 1000s of ns). For example, on a 2017
	// MacBook Pro, this case averages ~3245ns and on a gceworker it's
	// ~1200ns. At 50k replicas, that's on the order of a tenth of a second;
	// not enough to matter.
	//
	// On the other hand, successful (i.e. directory found) calls take ~23k
	// ns on my laptop, but only around 2.2k ns on the gceworker. Still,
	// even on the laptop, 50k replicas would only add 1.2s which is also
	// acceptable given that it'll happen only once.
	exists, err := exists(eng, path)
	if err != nil {
		return nil, errors.Wrap(err, "checking pre-migration sideloaded directory")
	}
	if exists {
		if err := eng.MkdirAll(filepath.Dir(newPath)); err != nil {
			return nil, errors.Wrap(err, "creating migrated sideloaded directory")
		}
		if err := eng.Rename(path, newPath); err != nil {
			return nil, errors.Wrap(err, "while migrating sideloaded directory")
		}
	}
	path = newPath

	ss := &diskSideloadStorage{
		dir:     path,
		eng:     eng,
		st:      st,
		limiter: limiter,
	}
	return ss, nil
}

func (ss *diskSideloadStorage) createDir() error {
	err := ss.eng.MkdirAll(ss.dir)
	ss.dirCreated = ss.dirCreated || err == nil
	return err
}

// Dir implements SideloadStorage.
func (ss *diskSideloadStorage) Dir() string {
	return ss.dir
}

// Put implements SideloadStorage.
func (ss *diskSideloadStorage) Put(ctx context.Context, index, term uint64, contents []byte) error {
	filename := ss.filename(ctx, index, term)
	// There's a chance the whole path is missing (for example after Clear()),
	// in which case handle that transparently.
	for {
		// Use 0644 since that's what RocksDB uses:
		// https://github.com/facebook/rocksdb/blob/56656e12d67d8a63f1e4c4214da9feeec2bd442b/env/env_posix.cc#L171
		if err := writeFileSyncing(ctx, filename, contents, ss.eng, 0644, ss.st, ss.limiter); err == nil {
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
func (ss *diskSideloadStorage) Get(ctx context.Context, index, term uint64) ([]byte, error) {
	filename := ss.filename(ctx, index, term)
	b, err := ss.eng.ReadFile(filename)
	if oserror.IsNotExist(err) {
		return nil, errSideloadedFileNotFound
	}
	return b, err
}

// Filename implements SideloadStorage.
func (ss *diskSideloadStorage) Filename(ctx context.Context, index, term uint64) (string, error) {
	return ss.filename(ctx, index, term), nil
}

func (ss *diskSideloadStorage) filename(ctx context.Context, index, term uint64) string {
	return filepath.Join(ss.dir, fmt.Sprintf("i%d.t%d", index, term))
}

// Purge implements SideloadStorage.
func (ss *diskSideloadStorage) Purge(ctx context.Context, index, term uint64) (int64, error) {
	return ss.purgeFile(ctx, ss.filename(ctx, index, term))
}

func (ss *diskSideloadStorage) fileSize(filename string) (int64, error) {
	info, err := ss.eng.Stat(filename)
	if err != nil {
		if oserror.IsNotExist(err) {
			return 0, errSideloadedFileNotFound
		}
		return 0, err
	}
	return info.Size(), nil
}

func (ss *diskSideloadStorage) purgeFile(ctx context.Context, filename string) (int64, error) {
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
func (ss *diskSideloadStorage) Clear(_ context.Context) error {
	err := ss.eng.RemoveAll(ss.dir)
	ss.dirCreated = ss.dirCreated && err != nil
	return err
}

// TruncateTo implements SideloadStorage.
func (ss *diskSideloadStorage) TruncateTo(
	ctx context.Context, firstIndex uint64,
) (bytesFreed, bytesRetained int64, _ error) {
	deletedAll := true
	if err := ss.forEach(ctx, func(index uint64, filename string) error {
		if index >= firstIndex {
			size, err := ss.fileSize(filename)
			if err != nil {
				return err
			}
			bytesRetained += size
			deletedAll = false
			return nil
		}
		fileSize, err := ss.purgeFile(ctx, filename)
		if err != nil {
			return err
		}
		bytesFreed += fileSize
		return nil
	}); err != nil {
		return 0, 0, err
	}

	if deletedAll {
		// The directory may not exist, or it may exist and have been empty.
		// Not worth trying to figure out which one, just try to delete.
		err := ss.eng.RemoveDir(ss.dir)
		if err != nil && !oserror.IsNotExist(err) {
			log.Infof(ctx, "unable to remove sideloaded dir %s: %v", ss.dir, err)
			err = nil // handled
		}
	}
	return bytesFreed, bytesRetained, nil
}

func (ss *diskSideloadStorage) forEach(
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
func (ss *diskSideloadStorage) String() string {
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
