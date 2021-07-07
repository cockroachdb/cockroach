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
	"io/ioutil"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gogo/protobuf/proto"
)

// PebbleFileRegistry keeps track of files for the data-FS and store-FS for Pebble (see encrypted_fs.go
// for high-level comment).
//
// It is created even when file registry is disabled, so that it can be used to ensure that
// a registry file did not exist previously, since that would indicate that disabling the registry
// can cause data loss.
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
	registryFilename string

	mu struct {
		syncutil.Mutex
		currProto *enginepb.FileRegistry
	}
}

const (
	fileRegistryFilename = "COCKROACHDB_REGISTRY"
)

// CheckNoRegistryFile checks that no registry file currently exists.
// CheckNoRegistryFile should be called if the file registry will not be used.
func (r *PebbleFileRegistry) CheckNoRegistryFile() error {
	// NB: We do not assign r.registryFilename if the registry will not be used.
	registryFilename := r.FS.PathJoin(r.DBDir, fileRegistryFilename)
	_, err := r.FS.Stat(registryFilename)
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
	r.mu.currProto = &enginepb.FileRegistry{}
	r.registryFilename = r.FS.PathJoin(r.DBDir, fileRegistryFilename)
	f, err := r.FS.Open(r.registryFilename)
	if oserror.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	var b []byte
	if b, err = ioutil.ReadAll(f); err != nil {
		return err
	}
	if err = protoutil.Unmarshal(b, r.mu.currProto); err != nil {
		return err
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
func (r *PebbleFileRegistry) SetFileEntry(filename string, entry *enginepb.FileEntry) error {
	// We choose not to store an entry for unencrypted files since the absence of
	// a file in the file registry implies that it is unencrypted.
	if entry != nil && entry.EnvType == enginepb.EnvType_Plaintext {
		return r.MaybeDeleteEntry(filename)
	}

	filename = r.tryMakeRelativePath(filename)
	newProto := &enginepb.FileRegistry{}

	r.mu.Lock()
	defer r.mu.Unlock()
	proto.Merge(newProto, r.mu.currProto)
	if newProto.Files == nil {
		newProto.Files = make(map[string]*enginepb.FileEntry)
	}
	newProto.Files[filename] = entry
	return r.writeRegistry(newProto)
}

// MaybeDeleteEntry deletes the entry for filename, if it exists, and persists the registry, if changed.
func (r *PebbleFileRegistry) MaybeDeleteEntry(filename string) error {
	filename = r.tryMakeRelativePath(filename)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.currProto.Files[filename] == nil {
		return nil
	}
	newProto := &enginepb.FileRegistry{}
	proto.Merge(newProto, r.mu.currProto)
	delete(newProto.Files, filename)
	return r.writeRegistry(newProto)
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
	newProto := &enginepb.FileRegistry{}
	proto.Merge(newProto, r.mu.currProto)
	if newProto.Files[src] == nil {
		delete(newProto.Files, dst)
	} else {
		newProto.Files[dst] = newProto.Files[src]
		delete(newProto.Files, src)
	}
	return r.writeRegistry(newProto)
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
	newProto := &enginepb.FileRegistry{}
	proto.Merge(newProto, r.mu.currProto)
	if newProto.Files[src] == nil {
		delete(newProto.Files, dst)
	} else {
		newProto.Files[dst] = newProto.Files[src]
	}
	return r.writeRegistry(newProto)
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
	r.mu.currProto = newProto
	return nil
}

func (r *PebbleFileRegistry) getRegistryCopy() *enginepb.FileRegistry {
	r.mu.Lock()
	defer r.mu.Unlock()
	rv := &enginepb.FileRegistry{}
	proto.Merge(rv, r.mu.currProto)
	return rv
}
