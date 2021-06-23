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
	// TODO(ayang): remove registryFilename when we deprecate the old registry
	registryFilename string
	registryPath     string

	mu struct {
		syncutil.Mutex
		// TODO(ayang): convert enginepb.FileRegistry to a regular struct
		// when we deprecate the old registry
		currProto      *enginepb.FileRegistry
		registryFile   vfs.File
		registryWriter *record.Writer
	}
}

const (
	// TODO(ayang): delete fileRegistryFilename when we deprecate the old registry
	fileRegistryFilename       = "COCKROACHDB_REGISTRY"
	encryptionRegistryFilename = "COCKROACHDB_ENCRYPTION_REGISTRY"
)

// CheckNoRegistryFile checks that no registry file currently exists.
// One of CheckNoRegistryFile or Load must be called exactly once,
// before any other functions.
func (r *PebbleFileRegistry) CheckNoRegistryFile() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if f, err := r.FS.Open(r.registryFilename); err == nil {
		_ = f.Close()
		return os.ErrExist
	}

	if f, err := r.FS.Open(r.registryPath); err == nil {
		_ = f.Close()
		return os.ErrExist
	}
	return nil
}

// Load loads the contents of the file registry from a file, if the file exists, else it is a noop.
// One of CheckNoRegistryFile or Load must be called exactly once,
// before any other functions.
func (r *PebbleFileRegistry) Load() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	f, err := r.FS.Open(r.registryFilename)
	hasOldRegistry := true
	if oserror.IsNotExist(err) {
		hasOldRegistry = false
	}
	if hasOldRegistry {
		if err != nil {
			return err
		}
		var b []byte
		if b, err = ioutil.ReadAll(f); err != nil {
			return err
		}
		if err = protoutil.Unmarshal(b, r.mu.currProto); err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		if r.mu.currProto.Version == enginepb.RegistryVersion_Records {
			return errors.Newf("old encryption registry with version Records should not exist\n")
		}
		// Delete all unnecessary entries to reduce registry size.
		for filename, entry := range r.mu.currProto.Files {
			if isUnencrypted(entry) {
				delete(r.mu.currProto.Files, filename)
			}
		}
		if r.mu.currProto.Version == enginepb.RegistryVersion_Base && r.MinVersion == enginepb.RegistryVersion_Records {
			if err := r.upgradeToRecordsVersion(); err != nil {
				return err
			}
		}
	} else {
		records, err := r.FS.Open(r.registryPath)
		hasNewRegistry := true
		if oserror.IsNotExist(err) {
			hasNewRegistry = false
		}
		if hasNewRegistry {
			if err != nil {
				return err
			}
			// We replay the records written to the registry to populate our in-memory map.
			rr := record.NewReader(records, 0 /* logNum */)
			rdr, err := rr.Next()
			if err == io.EOF {
				return errors.Newf("pebble new file registry exists but is empty\n")
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
				return errors.Newf("new encryption registry with version Base should not exist\n")
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
		}
	}
	// We create a new registry file and rotate to it since the existing one
	// is read-only.
	if err := r.createNewRegistryFile(); err != nil {
		return err
	}
	return nil
}

func (r *PebbleFileRegistry) init() {
	r.registryFilename = r.FS.PathJoin(r.DBDir, fileRegistryFilename)
	r.registryPath = r.FS.PathJoin(r.DBDir, encryptionRegistryFilename)
	r.mu.currProto = &enginepb.FileRegistry{Version: r.MinVersion}
}

// StopUsingOldRegistry is called to signal that the old file registry
// can be safely deleted and no longer used.
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
	if err := r.FS.Remove(r.registryFilename); err != nil {
		return err
	}
	// Create a new registry file to record the upgraded version.
	r.mu.currProto.Version = enginepb.RegistryVersion_Records
	return r.createNewRegistryFile()
}

// Shutdown closes the record writer and record file used for the registry.
// It must be called exactly once when a Pebble instance is closed.
func (r *PebbleFileRegistry) Shutdown() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mu.registryWriter.Close(); err != nil {
		return err
	}
	r.mu.registryWriter = nil
	if err := r.mu.registryFile.Close(); err != nil {
		return err
	}
	r.mu.registryFile = nil
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
func (r *PebbleFileRegistry) SetFileEntry(filename string, entry *enginepb.FileEntry) error {
	// We choose not to store an entry for unencrypted files since the absence of
	// a file in the file registry implies that it is unencrypted.
	if isUnencrypted(entry) {
		return nil
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
		return errors.Newf("entry should not be nil\n")
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
	// Failing to update registry is fatal.
	if err != nil {
		panic(err)
	}
	return nil
}

func (r *PebbleFileRegistry) updateRegistry(entry *enginepb.NamedFileEntry) {
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
		// Failing to update registry is fatal.
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
	tempPath := r.registryPath + tempFileExtension
	f, err := r.FS.Create(tempPath)
	if err != nil {
		return err
	}
	records := record.NewWriter(f)
	w, err := records.Next()
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(strconv.Itoa(int(r.mu.currProto.Version))))
	if err != nil {
		return err
	}
	for filename, entry := range r.mu.currProto.Files {
		b, err := protoutil.Marshal(&enginepb.NamedFileEntry{Filename: filename, Entry: entry})
		if err != nil {
			return err
		}
		w, err := records.Next()
		if err != nil {
			return err
		}
		_, err = w.Write(b)
		if err != nil {
			return err
		}
	}
	err = records.Flush()
	if err != nil {
		return err
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	err = r.FS.Rename(tempPath, r.registryPath)
	if err != nil {
		return err
	}
	fdir, err := r.FS.OpenDir(r.DBDir)
	if err != nil {
		return err
	}
	err = fdir.Sync()
	if err != nil {
		return err
	}
	err = fdir.Close()
	if err != nil {
		return err
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
	if err = SafeWriteToFile(r.FS, r.DBDir, r.registryFilename, b); err != nil {
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

func isUnencrypted(entry *enginepb.FileEntry) bool {
	return entry == nil || entry.EnvType == enginepb.EnvType_Plaintext
}
