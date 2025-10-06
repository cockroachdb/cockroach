// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
	"github.com/cockroachdb/redact"
)

// DefaultNumOldFileRegistryFiles is the default number of old registry files
// kept for debugging. Production callers should use this to initialize
// FileRegistry.NumOldRegistryFiles. The value of two is meant to slightly align
// the history with what we keep for the Pebble MANIFEST. We keep one Pebble
// MANIFEST, which is also 128MB in size, however the entry size per file in the
// MANIFEST is smaller (based on some unit test data it is about half), so we
// keep double the history for the file registry.
var DefaultNumOldFileRegistryFiles = envutil.EnvOrDefaultInt(
	"COCKROACH_STORE_NUM_OLD_FILE_REGISTRY_FILES", 2)

// CanRegistryElideFunc is a function that returns true for entries that can be
// elided instead of being written to the registry.
var CanRegistryElideFunc func(entry *enginepb.FileEntry) bool

const defaultSoftMaxRegistrySize = 128 << 20 // 128 MB

// FileRegistry keeps track of files for the data-FS and store-FS for
// encryption-at-rest (see encrypted_fs.go for high-level comment).
//
// It is created even when file registry is disabled, so that it can be
// used to ensure that a registry file did not exist previously, since
// that would indicate that disabling the registry can cause data loss.
//
// The records-based registry file written to disk contains a sequence
// of records: a marshaled enginepb.RegistryHeader byte slice followed
// by marshaled enginepb.RegistryUpdateBatch byte slices that correspond
// to a batch of updates to the file registry. The updates are replayed
// in Load.
type FileRegistry struct {
	// Initialize the following before calling Load().

	// The FS to write the file registry file.
	FS vfs.FS
	// The directory used by the DB. It is used to construct the name of the file registry file and
	// to turn absolute path names of files in this directory into relative path names. The latter
	// is done for compatibility with the file registry implemented for RocksDB, even though it
	// currently requires some potentially non-portable filepath manipulation.
	DBDir string
	// Is the DB read only.
	ReadOnly bool
	// The number of old file registry files to keep for debugging purposes.
	NumOldRegistryFiles int
	// SoftMaxSize configures the "soft max" file size of a registry file. Once
	// exceeded the registry may consider rolling over to a new file. Defaults
	// to defaultSoftMaxRegistrySize.
	SoftMaxSize int64

	// Implementation.

	writeMu struct {
		syncutil.Mutex
		mu struct {
			// Setters must hold both writeMu and writeMu.mu.
			// Getters: Can hold either writeMu or writeMu.mu.
			//
			// writeMu > writeMu.mu, since setters hold the former when acquiring
			// the latter. Setters should never hold the latter while doing IO. This
			// would block getters, that are using mu, if the IO got stuck, say due
			// to a disk stall -- we want getters (which may be doing read IO) to
			// not get stuck due to setters being stuck, since the read IO may
			// succeed due to the (page) cache.
			syncutil.RWMutex
			// entries stores the current state of the file registry. All values are
			// non-nil.
			entries map[string]*enginepb.FileEntry
		}
		// registryFile is the opened file for the records-based registry.
		registryFile vfs.File
		// registryWriter is a record.Writer for registryFile.
		registryWriter *record.Writer
		// rotationHelper holds state for deciding when to rotate to a new file
		// registry file and write a snapshot.
		rotationHelper record.RotationHelper
		// registryMarker is an atomic file marker used to denote which
		// of the records-based registry files is the current one. When
		// we rotate files, the marker is atomically moved to the new
		// file. It's guaranteed to be non-nil after Load.
		marker *atomicfs.Marker
		// registryFilename is the filename of the currently active
		// records-based registry file. If no file has been written yet,
		// this may be the empty string.
		registryFilename string

		// Obsolete files, ordered from oldest to newest.
		obsoleteRegistryFiles []string
	}
}

const (
	registryFilenameBase = "COCKROACHDB_REGISTRY"
	registryMarkerName   = "registry"
)

// checkNoRegistryFile checks that no registry file currently exists.
// checkNoRegistryFile should be called if the file registry will not be used.
func checkNoRegistryFile(fs vfs.FS, dbDir string) error {
	filename, err := atomicfs.ReadMarker(fs, dbDir, registryMarkerName)
	if oserror.IsNotExist(err) {
		// ReadMarker may return oserror.IsNotExist if the data
		// directory does not exist.
		return nil
	} else if err != nil {
		return err
	}
	if filename != "" {
		return oserror.ErrExist
	}
	return nil
}

// Load loads the contents of the file registry from a file, if the file
// exists, else it is a noop.  Load should be called exactly once if the
// file registry will be used.
func (r *FileRegistry) Load(ctx context.Context) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	if r.SoftMaxSize == 0 {
		r.SoftMaxSize = defaultSoftMaxRegistrySize
	}

	// Initialize private fields needed when the file registry will be used.
	func() {
		r.writeMu.mu.Lock()
		defer r.writeMu.mu.Unlock()
		r.writeMu.mu.entries = make(map[string]*enginepb.FileEntry)
	}()

	if err := r.loadRegistryFromFile(); err != nil {
		return err
	}

	// Find all old registry files, and remove some of them.
	if r.writeMu.registryFilename != "" {
		registryFileNum, err := r.parseRegistryFileName(r.writeMu.registryFilename)
		if err != nil {
			return err
		}
		files, err := r.FS.List(r.DBDir)
		if err != nil {
			return err
		}
		var obsoleteFiles []uint64
		for _, f := range files {
			f = r.FS.PathBase(f)
			if !strings.HasPrefix(f, registryFilenameBase) {
				continue
			}
			fileNum, err := r.parseRegistryFileName(f)
			if err != nil {
				return err
			}
			if fileNum > registryFileNum {
				// Delete it immediately, since newer than current registry file, so
				// must have crashed while creating it.
				err := r.FS.Remove(r.FS.PathJoin(r.DBDir, f))
				if err != nil {
					log.Errorf(ctx, "unable to remove registry file %s", f)
				}
			}
			if fileNum < registryFileNum {
				obsoleteFiles = append(obsoleteFiles, fileNum)
			}
		}
		sort.Slice(obsoleteFiles, func(i, j int) bool {
			return obsoleteFiles[i] < obsoleteFiles[j]
		})
		r.writeMu.obsoleteRegistryFiles = make([]string, 0, r.NumOldRegistryFiles+1)
		for _, f := range obsoleteFiles {
			r.writeMu.obsoleteRegistryFiles = append(r.writeMu.obsoleteRegistryFiles, makeRegistryFilename(f))
		}
		if err := r.tryRemoveOldRegistryFilesLocked(); err != nil {
			return err
		}
	}

	// Delete all unnecessary entries to reduce registry size.
	if err := r.maybeElideEntries(ctx); err != nil {
		return err
	}

	return nil
}

func (r *FileRegistry) loadRegistryFromFile() error {
	// The file registry uses an 'atomic marker' file to denote which of
	// the new records-based file registries is currently active.  It's
	// okay to load the marker unconditionally, because LocateMarker
	// succeeds even if the marker has never been placed.
	marker, currentFilename, err := atomicfs.LocateMarker(r.FS, r.DBDir, registryMarkerName)
	if err != nil {
		return err
	}
	r.writeMu.marker = marker
	// If the marker does not exist, currentFilename may be the
	// empty string. That's okay.
	r.writeMu.registryFilename = currentFilename

	// Atomic markers may accumulate obsolete files. Remove any obsolete
	// marker files as long as we're not in read-only mode.
	if !r.ReadOnly {
		if err := r.writeMu.marker.RemoveObsolete(); err != nil {
			return err
		}
	}

	if _, err := r.maybeLoadExistingRegistry(); err != nil {
		return err
	}
	return nil
}

func (r *FileRegistry) maybeLoadExistingRegistry() (bool, error) {
	if r.writeMu.registryFilename == "" {
		return false, nil
	}
	records, err := r.FS.Open(r.FS.PathJoin(r.DBDir, r.writeMu.registryFilename))
	if err != nil {
		return false, err
	}
	// We replay the records written to the registry to populate our in-memory map.
	rr := record.NewReader(records, 0 /* logNum */)
	rdr, err := rr.Next()
	if err == io.EOF {
		return false, errors.New("pebble new file registry exists but is missing a header")
	}
	if err != nil {
		return false, err
	}
	registryHeaderBytes, err := io.ReadAll(rdr)
	if err != nil {
		return false, err
	}
	registryHeader := &enginepb.RegistryHeader{}
	if err := protoutil.Unmarshal(registryHeaderBytes, registryHeader); err != nil {
		return false, err
	}
	// All registries of the base version should've been removed in 21.2 before
	// upgrade finalization.
	if registryHeader.Version == enginepb.RegistryVersion_Base {
		return false, errors.New("new encryption registry with version Base should not exist")
	}
	for {
		rdr, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return false, err
		}
		b, err := io.ReadAll(rdr)
		if err != nil {
			return false, err
		}
		batch := &enginepb.RegistryUpdateBatch{}
		if err := protoutil.Unmarshal(b, batch); err != nil {
			return false, err
		}
		r.applyBatch(batch)
	}
	if err := records.Close(); err != nil {
		return false, err
	}
	return true, nil
}

func (r *FileRegistry) maybeElideEntries(ctx context.Context) error {
	if r.ReadOnly {
		return nil
	}

	// Copy the filenames to a slice and sort it for deterministic
	// iteration order. This is helpful in tests and this function is a
	// one-time cost at startup.
	//
	// TODO(jackson): Rather than Stat-ing each file, we could
	// recursively List each directory and walk two lists of sorted
	// filenames. We should test a store with many files to see how much
	// the current approach slows node start.
	filenames := make([]string, 0, len(r.writeMu.mu.entries))
	for filename := range r.writeMu.mu.entries {
		filenames = append(filenames, filename)
	}
	sort.Strings(filenames)

	batch := &enginepb.RegistryUpdateBatch{}
	for _, filename := range filenames {
		entry, ok := r.writeMu.mu.entries[filename]
		if !ok {
			panic("entry disappeared from map")
		}

		// Some entries may be elided. This is used within
		// ccl/storageccl/engineccl to elide plaintext file entries.
		if CanRegistryElideFunc != nil && CanRegistryElideFunc(entry) {
			batch.DeleteEntry(filename)
			continue
		}

		// Files may exist within the registry but not on the filesystem
		// because registry updates are not atomic with filesystem edits.
		// Check if the file exists and elide the entry if the file does not
		// exist. This elision happens during store initialization, ensuring no
		// concurrent work should be ongoing which might have added an entry to
		// the file registry but not yet created the referenced file.
		path := filename
		if !filepath.IsAbs(path) {
			path = r.FS.PathJoin(r.DBDir, filename)
		}
		if _, err := r.FS.Stat(path); oserror.IsNotExist(err) {
			log.Infof(ctx, "eliding file registry entry %s", redact.SafeString(filename))
			batch.DeleteEntry(filename)
		}
	}
	return r.processBatchLocked(batch)
}

// GetFileEntry gets the file entry corresponding to filename, if there is one, else returns nil.
func (r *FileRegistry) GetFileEntry(filename string) *enginepb.FileEntry {
	filename = r.tryMakeRelativePath(filename)
	r.writeMu.mu.RLock()
	defer r.writeMu.mu.RUnlock()
	return r.writeMu.mu.entries[filename]
}

// SetFileEntry sets filename => entry in the registry map and persists the registry.
// It should not be called for entries corresponding to unencrypted files since the
// absence of a file in the file registry implies that it is unencrypted.
func (r *FileRegistry) SetFileEntry(filename string, entry *enginepb.FileEntry) error {
	// We don't need to store nil entries since that's the default zero value.
	if entry == nil {
		return r.MaybeDeleteEntry(filename)
	}

	filename = r.tryMakeRelativePath(filename)

	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	batch := &enginepb.RegistryUpdateBatch{}
	batch.PutEntry(filename, entry)
	return r.processBatchLocked(batch)
}

// MaybeDeleteEntry deletes the entry for filename, if it exists, and persists the registry, if changed.
func (r *FileRegistry) MaybeDeleteEntry(filename string) error {
	filename = r.tryMakeRelativePath(filename)

	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	if _, ok := r.writeMu.mu.entries[filename]; !ok {
		return nil
	}
	batch := &enginepb.RegistryUpdateBatch{}
	batch.DeleteEntry(filename)
	return r.processBatchLocked(batch)
}

// MaybeCopyEntry copies the entry under src to dst. If no entry exists
// for src but an entry exists for dst, dst's entry is deleted. These
// semantics are necessary for handling plaintext files that exist on
// the filesystem without a corresponding entry in the registry.
func (r *FileRegistry) MaybeCopyEntry(src, dst string) error {
	src = r.tryMakeRelativePath(src)
	dst = r.tryMakeRelativePath(dst)

	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	srcEntry, srcFound := r.writeMu.mu.entries[src]
	_, dstFound := r.writeMu.mu.entries[dst]
	if !srcFound && !dstFound {
		return nil
	}
	batch := &enginepb.RegistryUpdateBatch{}
	if !srcFound {
		batch.DeleteEntry(dst)
	} else {
		batch.PutEntry(dst, srcEntry)
	}
	return r.processBatchLocked(batch)
}

// MaybeLinkEntry copies the entry under src to dst, if src exists. If src does not exist, but dst
// exists, dst is deleted. Persists the registry if changed.
func (r *FileRegistry) MaybeLinkEntry(src, dst string) error {
	return r.MaybeCopyEntry(src, dst)
}

func (r *FileRegistry) tryMakeRelativePath(filename string) string {
	// Logic copied from file_registry.cc.
	//
	// This may not work for Windows.
	// TODO(sbhola): see if we can use filepath.Rel() here. filepath.Rel() will turn "/b/c"
	// relative to directory "/a" into "../b/c" which would not be compatible with what
	// we do in RocksDB, which is why we have copied the code below.

	dbDir := r.DBDir

	// Unclear why this should ever happen, but there is a test case for this in file_registry_test.cc
	// so we have duplicated this here.
	//
	// Check for the rare case when we're referring to the db directory itself (without slash).
	if filename == dbDir {
		return ""
	}
	if len(dbDir) > 0 && dbDir[len(dbDir)-1] != os.PathSeparator {
		dbDir = dbDir + string(os.PathSeparator)
	}
	if !strings.HasPrefix(filename, dbDir) {
		return filename
	}
	filename = filename[len(dbDir):]
	if len(filename) > 0 && filename[0] == os.PathSeparator {
		filename = filename[1:]
	}
	return filename
}

func (r *FileRegistry) processBatchLocked(batch *enginepb.RegistryUpdateBatch) error {
	if r.ReadOnly {
		return errors.New("cannot write file registry since db is read-only")
	}
	if batch.Empty() {
		return nil
	}
	if err := r.writeToRegistryFileLocked(batch); err != nil {
		panic(err)
	}
	r.applyBatch(batch)
	return nil
}

// processBatch processes a batch of updates to the file registry.
func (r *FileRegistry) applyBatch(batch *enginepb.RegistryUpdateBatch) {
	r.writeMu.mu.Lock()
	defer r.writeMu.mu.Unlock()
	for _, update := range batch.Updates {
		if update.Entry == nil {
			delete(r.writeMu.mu.entries, update.Filename)
		} else {
			if r.writeMu.mu.entries == nil {
				r.writeMu.mu.entries = make(map[string]*enginepb.FileEntry)
			}
			r.writeMu.mu.entries[update.Filename] = update.Entry
		}
	}
}

func (r *FileRegistry) writeToRegistryFileLocked(batch *enginepb.RegistryUpdateBatch) error {
	nilWriter := r.writeMu.registryWriter == nil

	// Create a new file registry file if one doesn't exist yet, or the current
	// one is too large.
	//
	// For largeness, we do not exclusively use r.SoftMaxSize size threshold
	// since a large store may have a large enough volume of files that a single
	// snapshot exceeds the size, resulting in rotation on every edit. This
	// slows the system down since each file creation or deletion writes a new
	// file registry snapshot. The primary goal of the size-based rollover logic
	// is to ensure that when reopening a DB, the number of edits that need to
	// be replayed on top of the snapshot is "sane". Rolling over to a new file
	// registry after each edit is not relevant to that goal.
	r.writeMu.rotationHelper.AddRecord(int64(len(batch.Updates)))

	// If we don't have a file yet, we need to create one. If we do and its size
	// exceeds the soft max, we may want to rotate. The record.RotationHelper
	// implements logic to determine when we should rotate.
	shouldRotate := nilWriter
	if !shouldRotate && r.writeMu.registryWriter.Size() > r.SoftMaxSize {
		shouldRotate = r.writeMu.rotationHelper.ShouldRotate(int64(len(r.writeMu.mu.entries)))
	}

	if shouldRotate {
		// If !nilWriter, exceeded the size threshold: create a new file registry
		// file to hopefully shrink the size of the file.
		if err := r.createNewRegistryFileLocked(); err != nil {
			if nilWriter {
				return errors.Wrap(err, "creating new registry file")
			} else {
				return errors.Wrap(err, "rotating registry file")
			}
		}
		// Successfully rotated.
		r.writeMu.rotationHelper.Rotate(int64(len(r.writeMu.mu.entries)))
	}
	w, err := r.writeMu.registryWriter.Next()
	if err != nil {
		return err
	}
	b, err := protoutil.Marshal(batch)
	if err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	if err := r.writeMu.registryWriter.Flush(); err != nil {
		return err
	}
	return r.writeMu.registryFile.Sync()
}

func makeRegistryFilename(iter uint64) string {
	return fmt.Sprintf("%s_%06d", registryFilenameBase, iter)
}

func (r *FileRegistry) createNewRegistryFileLocked() error {
	// Create a new registry file. It won't be active until the marker
	// is moved to the new filename.
	filename := makeRegistryFilename(r.writeMu.marker.NextIter())
	filepath := r.FS.PathJoin(r.DBDir, filename)
	f, err := r.FS.Create(filepath, EncryptionRegistryWriteCategory)
	if err != nil {
		return err
	}
	records := record.NewWriter(f)
	w, err := records.Next()
	if err != nil {
		return errors.CombineErrors(err, f.Close())
	}

	errFunc := func(err error) error {
		err1 := records.Close()
		err2 := f.Close()
		return errors.CombineErrors(err, errors.CombineErrors(err1, err2))
	}

	// Write the registry header as the first record in the registry file.
	registryHeader := &enginepb.RegistryHeader{
		Version: enginepb.RegistryVersion_Records,
	}
	b, err := protoutil.Marshal(registryHeader)
	if err != nil {
		return errFunc(err)
	}
	if _, err := w.Write(b); err != nil {
		return errFunc(err)
	}

	// Write a RegistryUpdateBatch containing the current state of the registry.
	batch := &enginepb.RegistryUpdateBatch{}
	for filename, entry := range r.writeMu.mu.entries {
		batch.PutEntry(filename, entry)
	}
	b, err = protoutil.Marshal(batch)
	if err != nil {
		return errFunc(err)
	}
	w, err = records.Next()
	if err != nil {
		return errFunc(err)
	}
	if _, err := w.Write(b); err != nil {
		return errFunc(err)
	}
	if err := records.Flush(); err != nil {
		return errFunc(err)
	}
	if err := f.Sync(); err != nil {
		return errFunc(err)
	}

	// Moving the marker to the new filename atomically switches to the
	// new file. Move handles syncing the data directory as well.
	if err := r.writeMu.marker.Move(filename); err != nil {
		return errors.Wrap(errFunc(err), "moving marker")
	}

	// Close and remove the previous registry file.
	{
		// Any errors in this block will be returned at the end of the
		// function, after we update the internal state to point to the
		// new filename (since we've already successfully installed it).
		err = r.closeRegistry()
		if r.writeMu.registryFilename != "" {
			r.writeMu.obsoleteRegistryFiles = append(r.writeMu.obsoleteRegistryFiles, r.writeMu.registryFilename)
			rmErr := r.tryRemoveOldRegistryFilesLocked()
			if rmErr != nil && !oserror.IsNotExist(rmErr) {
				err = errors.CombineErrors(err, rmErr)
			}
		}
	}

	r.writeMu.registryFile = f
	r.writeMu.registryWriter = records
	r.writeMu.registryFilename = filename
	return err
}

// GetRegistrySnapshot constructs an enginepb.FileRegistry representing a
// snapshot of the file registry state.
func (r *FileRegistry) GetRegistrySnapshot() *enginepb.FileRegistry {
	r.writeMu.mu.RLock()
	defer r.writeMu.mu.RUnlock()
	rv := &enginepb.FileRegistry{
		Version: enginepb.RegistryVersion_Records,
		Files:   make(map[string]*enginepb.FileEntry, len(r.writeMu.mu.entries)),
	}
	for filename, entry := range r.writeMu.mu.entries {
		ev := &enginepb.FileEntry{}
		*ev = *entry
		rv.Files[filename] = ev
	}
	return rv
}

// List returns a mapping of file in the registry to their enginepb.FileEntry.
func (r *FileRegistry) List() map[string]*enginepb.FileEntry {
	r.writeMu.mu.RLock()
	defer r.writeMu.mu.RUnlock()
	// Perform a defensive deep-copy of the internal map here, as there may be
	// modifications to it after it has been returned to the caller.
	m := make(map[string]*enginepb.FileEntry, len(r.writeMu.mu.entries))
	for k, v := range r.writeMu.mu.entries {
		m[k] = v
	}
	return m
}

func (r *FileRegistry) parseRegistryFileName(f string) (uint64, error) {
	fileNum, err := strconv.ParseUint(f[len(registryFilenameBase)+1:], 10, 64)
	if err != nil {
		return 0, errors.Errorf("could not parse number from file registry filename %s", f)
	}
	return fileNum, nil
}

func (r *FileRegistry) tryRemoveOldRegistryFilesLocked() error {
	n := len(r.writeMu.obsoleteRegistryFiles)
	if n <= r.NumOldRegistryFiles {
		return nil
	}
	m := n - r.NumOldRegistryFiles
	toDelete := r.writeMu.obsoleteRegistryFiles[:m]
	r.writeMu.obsoleteRegistryFiles = r.writeMu.obsoleteRegistryFiles[m:]
	var err error
	for _, f := range toDelete {
		rmErr := r.FS.Remove(r.FS.PathJoin(r.DBDir, f))
		if rmErr != nil {
			err = errors.CombineErrors(err, rmErr)
		}
	}
	return err
}

// Close closes the record writer and record file used for the registry.
// It should be called when a Pebble instance is closed.
func (r *FileRegistry) Close() error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	err := r.closeRegistry()
	err = errors.CombineErrors(err, r.writeMu.marker.Close())
	return err
}

func (r *FileRegistry) closeRegistry() error {
	var err1, err2 error
	if r.writeMu.registryWriter != nil {
		err1 = r.writeMu.registryWriter.Close()
		r.writeMu.registryWriter = nil
	}
	if r.writeMu.registryFile != nil {
		err2 = r.writeMu.registryFile.Close()
		r.writeMu.registryFile = nil
	}
	return errors.CombineErrors(err1, err2)
}
