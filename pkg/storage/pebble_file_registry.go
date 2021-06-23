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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gogo/protobuf/proto"
)

// CanRegistryElideFunc is a function that returns true for entries that can be
// elided instead of being written to the registry.
var CanRegistryElideFunc func(entry *enginepb.FileEntry) bool

const maxRegistrySize = 128 << 20 // 128 MB

// PebbleFileRegistry keeps track of files for the data-FS and store-FS for Pebble (see encrypted_fs.go
// for high-level comment).
//
// It is created even when file registry is disabled, so that it can be used to ensure that
// a registry file did not exist previously, since that would indicate that disabling the registry
// can cause data loss.
//
// The records-based registry file written to disk contains records that are
// marshaled enginepb.NamedFileEntry byte slices that correspond to each
// entry in mu.currProto.Files. A NamedFileEntry with a nil Entry indicates
// that a file was deleted from the registry. This information is used
// when the registry file is replayed in Load(). The exception to this is the
// first record, which is the byte encoding of the enginepb.RegistryVersion.
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

	// MinVersion is the minimum registry version that should be present.
	MinVersion enginepb.RegistryVersion

	// Implementation.
	// TODO(ayang): remove oldRegistryPath when we deprecate the old registry
	oldRegistryPath string
	registryPath    string

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
	}
}

const (
	// TODO(ayang): delete oldRegistryFilename when we deprecate the old registry
	oldRegistryFilename = "COCKROACHDB_REGISTRY"
	registryFilename    = "COCKROACHDB_ENCRYPTION_REGISTRY"
)

// CheckNoRegistryFile checks that no registry file currently exists.
// CheckNoRegistryFile should be called if the file registry will not be used.
func (r *PebbleFileRegistry) CheckNoRegistryFile() error {
	// NB: We do not assign r.oldRegistryPath if the registry will not be used.
	oldRegistryPath := r.FS.PathJoin(r.DBDir, oldRegistryFilename)
	_, err := r.FS.Stat(oldRegistryPath)
	if err == nil {
		return os.ErrExist
	}
	if !oserror.IsNotExist(err) {
		return err
	}

	// NB: We do not assign r.registryPath if the registry will not be used.
	registryPath := r.FS.PathJoin(r.DBDir, registryFilename)
	_, err = r.FS.Stat(registryPath)
	if err == nil {
		return os.ErrExist
	}
	if !oserror.IsNotExist(err) {
		return err
	}

	return nil
}

// Load loads the contents of the file registry from a file, if the file exists, else it is a noop.
// Load should be called exactly once if the file registry will be used.
func (r *PebbleFileRegistry) Load() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Initialize private fields needed when the file registry will be used.
	r.oldRegistryPath = r.FS.PathJoin(r.DBDir, oldRegistryFilename)
	r.registryPath = r.FS.PathJoin(r.DBDir, registryFilename)
	r.mu.currProto = &enginepb.FileRegistry{Version: r.MinVersion}

	// We treat the old registry file as the source of truth until the version
	// is finalized. At that point, we upgrade to the new records-based registry
	// file and delete the old registry file.
	ok, err := r.maybeLoadOldBaseRegistry()
	if err != nil {
		return err
	}
	if !ok {
		if err := r.maybeLoadNewRecordsRegistry(); err != nil {
			return err
		}
	}
	// We create a new registry file and rotate to it since the existing one
	// is read-only. Doing so also has the added side effect of removing
	// outdated entries for each file.
	if err := r.createNewRegistryFile(); err != nil {
		return err
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
		return false, err
	}
	if err = protoutil.Unmarshal(b, r.mu.currProto); err != nil {
		return false, err
	}
	if err = f.Close(); err != nil {
		return false, err
	}
	if r.mu.currProto.Version == enginepb.RegistryVersion_Records {
		return false, errors.Newf("old encryption registry with version Records should not exist")
	}
	// Delete all unnecessary entries to reduce registry size.
	if err := r.maybeElideEntries(); err != nil {
		return false, err
	}
	if r.mu.currProto.Version == enginepb.RegistryVersion_Base && r.MinVersion == enginepb.RegistryVersion_Records {
		if err := r.upgradeToRecordsVersion(); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (r *PebbleFileRegistry) maybeLoadNewRecordsRegistry() error {
	records, err := r.FS.Open(r.registryPath)
	if oserror.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	// We replay the records written to the registry to populate our in-memory map.
	rr := record.NewReader(records, 0 /* logNum */)
	rdr, err := rr.Next()
	if err == io.EOF {
		return errors.Newf("pebble new file registry exists but is empty")
	}
	if err != nil {
		return err
	}
	registryVersionBytes, err := ioutil.ReadAll(rdr)
	if err != nil {
		return err
	}
	registryVersion, err := strconv.Atoi(string(registryVersionBytes))
	if err != nil {
		return err
	}
	if enginepb.RegistryVersion(registryVersion) == enginepb.RegistryVersion_Base {
		return errors.Newf("new encryption registry with version Base should not exist")
	}
	for {
		rdr, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		namedEntryBytes, err := ioutil.ReadAll(rdr)
		if err != nil {
			return err
		}
		namedEntry := &enginepb.NamedFileEntry{}
		if err = protoutil.Unmarshal(namedEntryBytes, namedEntry); err != nil {
			return err
		}
		r.updateRegistry(namedEntry)
	}
	err = records.Close()
	if err != nil {
		return err
	}
	return nil
}

// StopUsingOldRegistry is called to signal that the old file registry
// is no longer needed and can be safely deleted.
// TODO(ayang): delete this function when we deprecate the old registry
func (r *PebbleFileRegistry) StopUsingOldRegistry() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.currProto.Version == enginepb.RegistryVersion_Records {
		return nil
	}
	return r.upgradeToRecordsVersion()
}

// TODO(ayang): delete this function when we deprecate the old registry
func (r *PebbleFileRegistry) upgradeToRecordsVersion() error {
	// Create a new registry file to record the upgraded version.
	r.mu.currProto.Version = enginepb.RegistryVersion_Records
	if err := r.createNewRegistryFile(); err != nil {
		return err
	}
	return r.FS.Remove(r.oldRegistryPath)
}

// TODO(ayang): delete this function when we deprecate the old registry
func (r *PebbleFileRegistry) maybeElideEntries() error {
	newProto := &enginepb.FileRegistry{}
	proto.Merge(newProto, r.mu.currProto)
	filesChanged := false
	for filename, entry := range newProto.Files {
		if CanRegistryElideFunc != nil && CanRegistryElideFunc(entry) {
			delete(newProto.Files, filename)
			filesChanged = true
		}
	}
	if filesChanged {
		return r.writeRegistry(newProto)
	}
	return nil
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
	return r.putEntry(filename, entry)
}

// MaybeDeleteEntry deletes the entry for filename, if it exists, and persists the registry, if changed.
func (r *PebbleFileRegistry) MaybeDeleteEntry(filename string) error {
	filename = r.tryMakeRelativePath(filename)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.currProto.Files[filename] == nil {
		return nil
	}
	return r.deleteEntry(filename)
}

// MaybeRenameEntry moves the entry under src to dst, if src exists. If src does not exist, but dst
// exists, dst is deleted. Persists the registry if changed.
func (r *PebbleFileRegistry) MaybeRenameEntry(src, dst string) error {
	src = r.tryMakeRelativePath(src)
	dst = r.tryMakeRelativePath(dst)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.currProto.Files[src] == nil && r.mu.currProto.Files[dst] == nil {
		return nil
	}
	if r.mu.currProto.Files[src] == nil {
		return r.deleteEntry(dst)
	}
	if err := r.putEntry(dst, r.mu.currProto.Files[src]); err != nil {
		return err
	}
	return r.deleteEntry(src)
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
	if r.mu.currProto.Files[src] == nil {
		return r.deleteEntry(dst)
	}
	return r.putEntry(dst, r.mu.currProto.Files[src])
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

func (r *PebbleFileRegistry) putEntry(filename string, entry *enginepb.FileEntry) error {
	if entry == nil {
		return errors.Newf("entry should not be nil")
	}
	return r.processNamedEntry(&enginepb.NamedFileEntry{Filename: filename, Entry: entry})
}

func (r *PebbleFileRegistry) deleteEntry(filename string) error {
	return r.processNamedEntry(&enginepb.NamedFileEntry{Filename: filename, Entry: nil})
}

func (r *PebbleFileRegistry) processNamedEntry(entry *enginepb.NamedFileEntry) error {
	if r.ReadOnly {
		return errors.Newf("cannot write file registry since db is read-only")
	}
	r.updateRegistry(entry)
	var err error
	if r.mu.registryWriter == nil || r.mu.registryWriter.Size() > maxRegistrySize {
		err = r.createNewRegistryFile()
	} else {
		err = r.writeToRegistryFile(entry)
	}
	// Failing to update new registry is fatal.
	if err != nil {
		panic(err)
	}
	return nil
}

func (r *PebbleFileRegistry) updateRegistry(entry *enginepb.NamedFileEntry) {
	// TODO(ayang): delete this if block when we deprecate the old registry
	if r.mu.currProto.Version == enginepb.RegistryVersion_Base {
		newProto := &enginepb.FileRegistry{}
		proto.Merge(newProto, r.mu.currProto)
		if entry.Entry == nil {
			delete(newProto.Files, entry.Filename)
		} else {
			if newProto.Files == nil {
				newProto.Files = make(map[string]*enginepb.FileEntry)
			}
			newProto.Files[entry.Filename] = entry.Entry
		}
		// Failing to update old registry is fatal.
		if err := r.writeRegistry(newProto); err != nil {
			panic(err)
		}
		r.mu.currProto = newProto
		return
	}
	if entry.Entry == nil {
		delete(r.mu.currProto.Files, entry.Filename)
	} else {
		if r.mu.currProto.Files == nil {
			r.mu.currProto.Files = make(map[string]*enginepb.FileEntry)
		}
		r.mu.currProto.Files[entry.Filename] = entry.Entry
	}
}

func (r *PebbleFileRegistry) createNewRegistryFile() error {
	// Create a temporary file for the new registry file.
	tempPath := r.registryPath + tempFileExtension
	f, err := r.FS.Create(tempPath)
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

	// Write the version number as the first record in the registry file.
	_, err = w.Write([]byte(strconv.Itoa(int(r.mu.currProto.Version))))
	if err != nil {
		return errFunc(err)
	}

	// Write a NamedFileEntry for each file as a record in the registry file.
	for filename, entry := range r.mu.currProto.Files {
		b, err := protoutil.Marshal(&enginepb.NamedFileEntry{Filename: filename, Entry: entry})
		if err != nil {
			return errFunc(err)
		}
		w, err := records.Next()
		if err != nil {
			return errFunc(err)
		}
		if _, err = w.Write(b); err != nil {
			return errFunc(err)
		}
	}
	if err := records.Flush(); err != nil {
		return errFunc(err)
	}
	if err := f.Sync(); err != nil {
		return errFunc(err)
	}

	// Close and replace the current registry file with the temporary file.
	if err := r.closeRegistry(); err != nil {
		return errFunc(err)
	}
	if err := r.FS.Rename(tempPath, r.registryPath); err != nil {
		return errFunc(err)
	}
	fdir, err := r.FS.OpenDir(r.DBDir)
	if err != nil {
		return errFunc(err)
	}
	if err := fdir.Sync(); err != nil {
		return errFunc(err)
	}
	if err := fdir.Close(); err != nil {
		return errFunc(err)
	}
	r.mu.registryFile = f
	r.mu.registryWriter = records
	return nil
}

func (r *PebbleFileRegistry) writeToRegistryFile(entry *enginepb.NamedFileEntry) error {
	w, err := r.mu.registryWriter.Next()
	if err != nil {
		return err
	}
	b, err := protoutil.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	if err != nil {
		return err
	}
	err = r.mu.registryWriter.Flush()
	if err != nil {
		return err
	}
	return r.mu.registryFile.Sync()
}

// TODO(ayang): delete this function when we deprecate the old registry
func (r *PebbleFileRegistry) writeRegistry(newProto *enginepb.FileRegistry) error {
	if r.ReadOnly {
		return fmt.Errorf("cannot write file registry since db is read-only")
	}
	b, err := protoutil.Marshal(newProto)
	if err != nil {
		return err
	}
	if err = SafeWriteToFile(r.FS, r.DBDir, r.oldRegistryPath, b); err != nil {
		return err
	}
	return nil
}

func (r *PebbleFileRegistry) getRegistryCopy() *enginepb.FileRegistry {
	r.mu.Lock()
	defer r.mu.Unlock()
	rv := &enginepb.FileRegistry{}
	proto.Merge(rv, r.mu.currProto)
	return rv
}

// Close closes the record writer and record file used for the registry.
// It must be called exactly once when a Pebble instance is closed.
func (r *PebbleFileRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeRegistry()
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
