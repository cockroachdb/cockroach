// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"os"

	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
)

// checkpointOptions hold the optional parameters to construct checkpoint
// snapshots.
type checkpointOptions struct {
	// flushWAL set to true will force a flush and sync of the WAL prior to
	// checkpointing.
	flushWAL bool
}

// CheckpointOption set optional parameters used by `DB.Checkpoint`.
type CheckpointOption func(*checkpointOptions)

// WithFlushedWAL enables flushing and syncing the WAL prior to constructing a
// checkpoint. This guarantees that any writes committed before calling
// DB.Checkpoint will be part of that checkpoint.
//
// Note that this setting can only be useful in cases when some writes are
// performed with Sync = false. Otherwise, the guarantee will already be met.
//
// Passing this option is functionally equivalent to calling
// DB.LogData(nil, Sync) right before DB.Checkpoint.
func WithFlushedWAL() CheckpointOption {
	return func(opt *checkpointOptions) {
		opt.flushWAL = true
	}
}

// mkdirAllAndSyncParents creates destDir and any of its missing parents.
// Those missing parents, as well as the closest existing ancestor, are synced.
// Returns a handle to the directory created at destDir.
func mkdirAllAndSyncParents(fs vfs.FS, destDir string) (vfs.File, error) {
	// Collect paths for all directories between destDir (excluded) and its
	// closest existing ancestor (included).
	var parentPaths []string
	foundExistingAncestor := false
	for parentPath := fs.PathDir(destDir); parentPath != "."; parentPath = fs.PathDir(parentPath) {
		parentPaths = append(parentPaths, parentPath)
		_, err := fs.Stat(parentPath)
		if err == nil {
			// Exit loop at the closest existing ancestor.
			foundExistingAncestor = true
			break
		}
		if !oserror.IsNotExist(err) {
			return nil, err
		}
	}
	// Handle empty filesystem edge case.
	if !foundExistingAncestor {
		parentPaths = append(parentPaths, "")
	}
	// Create destDir and any of its missing parents.
	if err := fs.MkdirAll(destDir, 0755); err != nil {
		return nil, err
	}
	// Sync all the parent directories up to the closest existing ancestor,
	// included.
	for _, parentPath := range parentPaths {
		parentDir, err := fs.OpenDir(parentPath)
		if err != nil {
			return nil, err
		}
		err = parentDir.Sync()
		if err != nil {
			_ = parentDir.Close()
			return nil, err
		}
		err = parentDir.Close()
		if err != nil {
			return nil, err
		}
	}
	return fs.OpenDir(destDir)
}

// Checkpoint constructs a snapshot of the DB instance in the specified
// directory. The WAL, MANIFEST, OPTIONS, and sstables will be copied into the
// snapshot. Hard links will be used when possible. Beware of the significant
// space overhead for a checkpoint if hard links are disabled. Also beware that
// even if hard links are used, the space overhead for the checkpoint will
// increase over time as the DB performs compactions.
func (d *DB) Checkpoint(
	destDir string, opts ...CheckpointOption,
) (
	ckErr error, /* used in deferred cleanup */
) {
	opt := &checkpointOptions{}
	for _, fn := range opts {
		fn(opt)
	}

	if _, err := d.opts.FS.Stat(destDir); !oserror.IsNotExist(err) {
		if err == nil {
			return &os.PathError{
				Op:   "checkpoint",
				Path: destDir,
				Err:  oserror.ErrExist,
			}
		}
		return err
	}

	if opt.flushWAL && !d.opts.DisableWAL {
		// Write an empty log-data record to flush and sync the WAL.
		if err := d.LogData(nil /* data */, Sync); err != nil {
			return err
		}
	}

	// Disable file deletions.
	d.mu.Lock()
	d.disableFileDeletions()
	defer func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.enableFileDeletions()
	}()

	// TODO(peter): RocksDB provides the option to roll the manifest if the
	// MANIFEST size is too large. Should we do this too?

	// Lock the manifest before getting the current version. We need the
	// length of the manifest that we read to match the current version that
	// we read, otherwise we might copy a versionEdit not reflected in the
	// sstables we copy/link.
	d.mu.versions.logLock()
	// Get the unflushed log files, the current version, and the current manifest
	// file number.
	memQueue := d.mu.mem.queue
	current := d.mu.versions.currentVersion()
	formatVers := d.mu.formatVers.vers
	manifestFileNum := d.mu.versions.manifestFileNum
	manifestSize := d.mu.versions.manifest.Size()
	optionsFileNum := d.optionsFileNum

	// Release the manifest and DB.mu so we don't block other operations on
	// the database.
	d.mu.versions.logUnlock()
	d.mu.Unlock()

	// Wrap the normal filesystem with one which wraps newly created files with
	// vfs.NewSyncingFile.
	fs := syncingFS{
		FS: d.opts.FS,
		syncOpts: vfs.SyncingFileOptions{
			NoSyncOnClose: d.opts.NoSyncOnClose,
			BytesPerSync:  d.opts.BytesPerSync,
		},
	}

	// Create the dir and its parents (if necessary), and sync them.
	var dir vfs.File
	defer func() {
		if dir != nil {
			_ = dir.Close()
		}
		if ckErr != nil {
			// Attempt to cleanup on error.
			paths, _ := fs.List(destDir)
			for _, path := range paths {
				_ = fs.Remove(path)
			}
			_ = fs.Remove(destDir)
		}
	}()
	dir, ckErr = mkdirAllAndSyncParents(fs, destDir)
	if ckErr != nil {
		return ckErr
	}

	{
		// Link or copy the OPTIONS.
		srcPath := base.MakeFilepath(fs, d.dirname, fileTypeOptions, optionsFileNum)
		destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
		ckErr = vfs.LinkOrCopy(fs, srcPath, destPath)
		if ckErr != nil {
			return ckErr
		}
	}

	{
		// Set the format major version in the destination directory.
		var versionMarker *atomicfs.Marker
		versionMarker, _, ckErr = atomicfs.LocateMarker(fs, destDir, formatVersionMarkerName)
		if ckErr != nil {
			return ckErr
		}

		// We use the marker to encode the active format version in the
		// marker filename. Unlike other uses of the atomic marker,
		// there is no file with the filename `formatVers.String()` on
		// the filesystem.
		ckErr = versionMarker.Move(formatVers.String())
		if ckErr != nil {
			return ckErr
		}
		ckErr = versionMarker.Close()
		if ckErr != nil {
			return ckErr
		}
	}

	{
		// Copy the MANIFEST, and create a pointer to it. We copy rather
		// than link because additional version edits added to the
		// MANIFEST after we took our snapshot of the sstables will
		// reference sstables that aren't in our checkpoint. For a
		// similar reason, we need to limit how much of the MANIFEST we
		// copy.
		srcPath := base.MakeFilepath(fs, d.dirname, fileTypeManifest, manifestFileNum)
		destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
		ckErr = vfs.LimitedCopy(fs, srcPath, destPath, manifestSize)
		if ckErr != nil {
			return ckErr
		}

		// Recent format versions use an atomic marker for setting the
		// active manifest. Older versions use the CURRENT file. The
		// setCurrentFunc function will return a closure that will
		// take the appropriate action for the database's format
		// version.
		var manifestMarker *atomicfs.Marker
		manifestMarker, _, ckErr = atomicfs.LocateMarker(fs, destDir, manifestMarkerName)
		if ckErr != nil {
			return ckErr
		}
		ckErr = setCurrentFunc(formatVers, manifestMarker, fs, destDir, dir)(manifestFileNum)
		if ckErr != nil {
			return ckErr
		}
		ckErr = manifestMarker.Close()
		if ckErr != nil {
			return ckErr
		}
	}

	// Link or copy the sstables.
	for l := range current.Levels {
		iter := current.Levels[l].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			srcPath := base.MakeFilepath(fs, d.dirname, fileTypeTable, f.FileNum)
			destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
			ckErr = vfs.LinkOrCopy(fs, srcPath, destPath)
			if ckErr != nil {
				return ckErr
			}
		}
	}

	// Copy the WAL files. We copy rather than link because WAL file recycling
	// will cause the WAL files to be reused which would invalidate the
	// checkpoint.
	for i := range memQueue {
		logNum := memQueue[i].logNum
		if logNum == 0 {
			continue
		}
		srcPath := base.MakeFilepath(fs, d.walDirname, fileTypeLog, logNum)
		destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
		ckErr = vfs.Copy(fs, srcPath, destPath)
		if ckErr != nil {
			return ckErr
		}
	}

	// Sync and close the checkpoint directory.
	ckErr = dir.Sync()
	if ckErr != nil {
		return ckErr
	}
	ckErr = dir.Close()
	dir = nil
	return ckErr
}
