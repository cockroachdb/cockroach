// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
	"github.com/gogo/protobuf/proto"
)

// CanRegistryElideFunc is a function that returns true for entries that can be
// elided instead of being written to the registry.
var CanRegistryElideFunc func(entry *enginepb.FileEntry) bool

const maxRegistrySize = 128 << 20 // 128 MB

// PebbleFileRegistry keeps track of files for the data-FS and store-FS
// for Pebble (see encrypted_fs.go for high-level comment).
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
type PebbleFileRegistry struct {
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

	// Implementation.
	// TODO(ayang): remove oldRegistryPath when we deprecate the old registry
	oldRegistryPath string

	mu struct {
		syncutil.Mutex
		// currProto stores the current state of the file registry.
		// TODO(ayang): convert enginepb.FileRegistry to a regular struct
		// when we deprecate the old registry and rename currProto
		currProto *enginepb.FileRegistry
		// registryFile is the opened file for the records-based registry.
		registryFile vfs.File
		// registryWriter is a record.Writer for registryFile.
		registryWriter *record.Writer
		// registryMarker is an atomic file marker used to denote which
		// of the records-based registry files is the current one. When
		// we rotate files, the marker is atomically moved to the new
		// file. It's guaranteed to be non-nil after Load.
		marker *atomicfs.Marker
		// registryFilename is the filename of the currently active
		// records-based registry file. If no file has been written yet,
		// this may be the empty string.
		registryFilename string
	}
}

const (
	// TODO(ayang): mark COCKROACHDB_REGISTRY as deprecated so it isn't reused
	registryFilename   = "COCKROACHDB_REGISTRY"
	registryMarkerName = "registry"
)

// CheckNoRegistryFile checks that no registry file currently exists.
// CheckNoRegistryFile should be called if the file registry will not be used.
func (r *PebbleFileRegistry) CheckNoRegistryFile() error {
	if err := r.checkNoBaseRegistry(); err != nil {
		return err
	}
	if err := r.checkNoRecordsRegistry(); err != nil {
		return err
	}
	return nil
}

func (r *PebbleFileRegistry) checkNoBaseRegistry() error {
	// NB: We do not assign r.oldRegistryPath if the registry will not be used.
	oldRegistryPath := r.FS.PathJoin(r.DBDir, registryFilename)
	_, err := r.FS.Stat(oldRegistryPath)
	if err == nil {
		return os.ErrExist
	}
	if !oserror.IsNotExist(err) {
		return err
	}
	return nil
}

func (r *PebbleFileRegistry) checkNoRecordsRegistry() error {
	filename, err := atomicfs.ReadMarker(r.FS, r.DBDir, registryMarkerName)
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
func (r *PebbleFileRegistry) Load() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Initialize private fields needed when the file registry will be used.
	r.oldRegistryPath = r.FS.PathJoin(r.DBDir, registryFilename)
	r.mu.currProto = &enginepb.FileRegistry{}

	if err := r.loadRegistryFromFile(); err != nil {
		return err
	}

	// Delete all unnecessary entries to reduce registry size.
	if err := r.maybeElideEntries(); err != nil {
		return err
	}

	return nil
}

func (r *PebbleFileRegistry) loadRegistryFromFile() error {
	// The file registry uses an 'atomic marker' file to denote which of
	// the new records-based file registries is currently active.  It's
	// okay to load the marker unconditionally, because LocateMarker
	// succeeds even if the marker has never been placed.
	marker, currentFilename, err := atomicfs.LocateMarker(r.FS, r.DBDir, registryMarkerName)
	if err != nil {
		return err
	}
	r.mu.marker = marker
	// If the marker does not exist, currentFilename may be the
	// empty string. That's okay.
	r.mu.registryFilename = currentFilename

	// Atomic markers may accumulate obsolete files. Remove any obsolete
	// marker files as long as we're not in read-only mode.
	if !r.ReadOnly {
		if err := r.mu.marker.RemoveObsolete(); err != nil {
			return err
		}
	}

	// We treat the old registry file as the source of truth until the version
	// is finalized. At that point, we upgrade to the new records-based registry
	// file and delete the old registry file.
	ok, err := r.maybeLoadOldBaseRegistry()
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	ok, err = r.maybeLoadNewRecordsRegistry()
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	// If encryption-at-rest was not previously enabled, we check the storage min
	// version to determine whether we still need to create an old base registry.
	target := clusterversion.ByKey(clusterversion.RecordsBasedRegistry)
	ok, err = MinVersionIsAtLeastTargetVersion(r.FS, r.DBDir, target)
	if err != nil {
		return err
	}
	if ok {
		r.mu.currProto.SetVersion(enginepb.RegistryVersion_Records)
	}
	return nil
}

func (r *PebbleFileRegistry) maybeLoadOldBaseRegistry() (bool, error) {
	f, err := r.FS.Open(r.oldRegistryPath)
	if oserror.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var b []byte
	if b, err = ioutil.ReadAll(f); err != nil {
		return false, errors.CombineErrors(err, f.Close())
	}
	if err := f.Close(); err != nil {
		return false, err
	}
	if err := protoutil.Unmarshal(b, r.mu.currProto); err != nil {
		return false, err
	}
	if r.mu.currProto.Version == enginepb.RegistryVersion_Records {
		return false, errors.New("old encryption registry with version Records should not exist")
	}
	return true, nil
}

func (r *PebbleFileRegistry) maybeLoadNewRecordsRegistry() (bool, error) {
	if r.mu.registryFilename == "" {
		return false, nil
	}
	records, err := r.FS.Open(r.FS.PathJoin(r.DBDir, r.mu.registryFilename))
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
	registryHeaderBytes, err := ioutil.ReadAll(rdr)
	if err != nil {
		return false, err
	}
	registryHeader := &enginepb.RegistryHeader{}
	if err := protoutil.Unmarshal(registryHeaderBytes, registryHeader); err != nil {
		return false, err
	}
	// Since we only load the new registry if the old registry does not exist,
	// we should never load a new registry that has version Base.
	if registryHeader.Version == enginepb.RegistryVersion_Base {
		return false, errors.New("new encryption registry with version Base should not exist")
	}
	r.mu.currProto.SetVersion(registryHeader.Version)
	for {
		rdr, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return false, err
		}
		b, err := ioutil.ReadAll(rdr)
		if err != nil {
			return false, err
		}
		batch := &enginepb.RegistryUpdateBatch{}
		if err := protoutil.Unmarshal(b, batch); err != nil {
			return false, err
		}
		r.mu.currProto.ProcessBatch(batch)
	}
	if err := records.Close(); err != nil {
		return false, err
	}
	return true, nil
}

// StopUsingOldRegistry is called to signal that the old file registry
// is no longer needed and can be safely deleted.
// TODO(ayang): delete this function when we deprecate the old registry
func (r *PebbleFileRegistry) StopUsingOldRegistry() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.upgradeToRecordsVersion()
}

// UpgradedToRecordsVersion returns whether the file registry has completed
// its upgrade to the Records version of the file registry.
func (r *PebbleFileRegistry) UpgradedToRecordsVersion() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.currProto.Version == enginepb.RegistryVersion_Records
}

func (r *PebbleFileRegistry) maybeElideEntries() error {
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
	filenames := make([]string, 0, len(r.mu.currProto.Files))
	for filename := range r.mu.currProto.Files {
		filenames = append(filenames, filename)
	}
	sort.Strings(filenames)

	batch := &enginepb.RegistryUpdateBatch{}
	for _, filename := range filenames {
		entry := r.mu.currProto.Files[filename]

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
			batch.DeleteEntry(filename)
		}
	}
	return r.processBatchLocked(batch)
}

// GetFileEntry gets the file entry corresponding to filename, if there is one, else returns nil.
func (r *PebbleFileRegistry) GetFileEntry(filename string) *enginepb.FileEntry {
	filename = r.tryMakeRelativePath(filename)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.currProto.Files[filename]
}

// SetFileEntry sets filename => entry in the registry map and persists the registry.
// It should not be called for entries corresponding to unencrypted files since the
// absence of a file in the file registry implies that it is unencrypted.
func (r *PebbleFileRegistry) SetFileEntry(filename string, entry *enginepb.FileEntry) error {
	// We don't need to store nil entries since that's the default zero value.
	if entry == nil {
		return r.MaybeDeleteEntry(filename)
	}

	filename = r.tryMakeRelativePath(filename)

	r.mu.Lock()
	defer r.mu.Unlock()
	batch := &enginepb.RegistryUpdateBatch{}
	batch.PutEntry(filename, entry)
	return r.processBatchLocked(batch)
}

// MaybeDeleteEntry deletes the entry for filename, if it exists, and persists the registry, if changed.
func (r *PebbleFileRegistry) MaybeDeleteEntry(filename string) error {
	filename = r.tryMakeRelativePath(filename)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.currProto.Files[filename] == nil {
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
func (r *PebbleFileRegistry) MaybeCopyEntry(src, dst string) error {
	src = r.tryMakeRelativePath(src)
	dst = r.tryMakeRelativePath(dst)

	r.mu.Lock()
	defer r.mu.Unlock()
	srcEntry := r.mu.currProto.Files[src]
	if srcEntry == nil && r.mu.currProto.Files[dst] == nil {
		return nil
	}
	batch := &enginepb.RegistryUpdateBatch{}
	if srcEntry == nil {
		batch.DeleteEntry(dst)
	} else {
		batch.PutEntry(dst, srcEntry)
	}
	return r.processBatchLocked(batch)
}

// MaybeLinkEntry copies the entry under src to dst, if src exists. If src does not exist, but dst
// exists, dst is deleted. Persists the registry if changed.
func (r *PebbleFileRegistry) MaybeLinkEntry(src, dst string) error {
	src = r.tryMakeRelativePath(src)
	dst = r.tryMakeRelativePath(dst)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.currProto.Files[src] == nil && r.mu.currProto.Files[dst] == nil {
		return nil
	}
	batch := &enginepb.RegistryUpdateBatch{}
	if r.mu.currProto.Files[src] == nil {
		batch.DeleteEntry(dst)
	} else {
		batch.PutEntry(dst, r.mu.currProto.Files[src])
	}
	return r.processBatchLocked(batch)
}

func (r *PebbleFileRegistry) tryMakeRelativePath(filename string) string {
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

// TODO(ayang): delete this function when we deprecate the old registry
func (r *PebbleFileRegistry) upgradeToRecordsVersion() error {
	if r.mu.currProto.Version == enginepb.RegistryVersion_Records {
		return nil
	}

	dir, err := r.FS.OpenDir(r.DBDir)
	if err != nil {
		return err
	}
	defer dir.Close()

	// Create a new registry file to record the upgraded version.
	r.mu.currProto.SetVersion(enginepb.RegistryVersion_Records)
	if err := r.createNewRegistryFile(); err != nil {
		return err
	}
	if err := r.FS.Remove(r.oldRegistryPath); err != nil && !oserror.IsNotExist(err) {
		return err
	}
	// We need to sync the removal of the old registry because the
	// presence of the old registry will cause future starts to read
	// from the old registry.
	if err := dir.Sync(); err != nil {
		// Fsync errors must be fatal.
		panic(errors.Wrap(err, "syncing database directory"))
	}
	return nil
}

func (r *PebbleFileRegistry) processBatchLocked(batch *enginepb.RegistryUpdateBatch) error {
	if r.ReadOnly {
		return errors.New("cannot write file registry since db is read-only")
	}
	if batch.Empty() {
		return nil
	}
	// For durability reasons, we persist the changes to disk first before we
	// update the in-memory registry. Any error during persisting is fatal.
	if r.mu.currProto.Version == enginepb.RegistryVersion_Base {
		newProto := &enginepb.FileRegistry{}
		proto.Merge(newProto, r.mu.currProto)
		newProto.ProcessBatch(batch)
		if err := r.rewriteOldRegistry(newProto); err != nil {
			panic(err)
		}
	}
	if err := r.writeToRegistryFile(batch); err != nil {
		panic(err)
	}
	r.mu.currProto.ProcessBatch(batch)
	return nil
}

// TODO(ayang): delete this function when we deprecate the old registry
func (r *PebbleFileRegistry) rewriteOldRegistry(newProto *enginepb.FileRegistry) error {
	b, err := protoutil.Marshal(newProto)
	if err != nil {
		return err
	}
	if err := SafeWriteToFile(r.FS, r.DBDir, r.oldRegistryPath, b); err != nil {
		return err
	}
	return nil
}

func (r *PebbleFileRegistry) writeToRegistryFile(batch *enginepb.RegistryUpdateBatch) error {
	// Create a new file registry file if one doesn't exist yet.
	if r.mu.registryWriter == nil {
		if err := r.createNewRegistryFile(); err != nil {
			return errors.Wrap(err, "creating new registry file")
		}
	}
	w, err := r.mu.registryWriter.Next()
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
	if err := r.mu.registryWriter.Flush(); err != nil {
		return err
	}
	if err := r.mu.registryFile.Sync(); err != nil {
		return err
	}
	// Create a new file registry file to hopefully shrink the size of the file
	// if we have exceeded the max registry size.
	if r.mu.registryWriter.Size() > maxRegistrySize {
		if err := r.createNewRegistryFile(); err != nil {
			return errors.Wrap(err, "rotating registry file")
		}
	}
	return nil
}

func makeRegistryFilename(iter uint64) string {
	return fmt.Sprintf("%s_%06d", registryFilename, iter)
}

func (r *PebbleFileRegistry) createNewRegistryFile() error {
	// Create a new registry file. It won't be active until the marker
	// is moved to the new filename.
	filename := makeRegistryFilename(r.mu.marker.NextIter())
	filepath := r.FS.PathJoin(r.DBDir, filename)
	f, err := r.FS.Create(filepath)
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
		Version: r.mu.currProto.Version,
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
	for filename, entry := range r.mu.currProto.Files {
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
	if err := r.mu.marker.Move(filename); err != nil {
		return errors.Wrap(errFunc(err), "moving marker")
	}

	// Close and remove the previous registry file.
	{
		// Any errors in this block will be returned at the end of the
		// function, after we update the internal state to point to the
		// new filename (since we've already successfully installed it).
		err = r.closeRegistry()
		if err == nil && r.mu.registryFilename != "" {
			rmErr := r.FS.Remove(r.FS.PathJoin(r.DBDir, r.mu.registryFilename))
			if rmErr != nil && !oserror.IsNotExist(rmErr) {
				err = errors.CombineErrors(err, rmErr)
			}
		}
	}

	r.mu.registryFile = f
	r.mu.registryWriter = records
	r.mu.registryFilename = filename

	return err
}

func (r *PebbleFileRegistry) getRegistryCopy() *enginepb.FileRegistry {
	r.mu.Lock()
	defer r.mu.Unlock()
	rv := &enginepb.FileRegistry{}
	proto.Merge(rv, r.mu.currProto)
	return rv
}

// Close closes the record writer and record file used for the registry.
// It should be called when a Pebble instance is closed.
func (r *PebbleFileRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	err := r.closeRegistry()
	err = errors.CombineErrors(err, r.mu.marker.Close())
	return err
}

func (r *PebbleFileRegistry) closeRegistry() error {
	var err1, err2 error
	if r.mu.registryWriter != nil {
		err1 = r.mu.registryWriter.Close()
		r.mu.registryWriter = nil
	}
	if r.mu.registryFile != nil {
		err2 = r.mu.registryFile.Close()
		r.mu.registryFile = nil
	}
	return errors.CombineErrors(err1, err2)
}
