package engine

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gogo/protobuf/proto"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

// See encrypted_fs.go for high-level context.
//
// FileRegistry keeps track of files for the data-FS and store-FS.
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

	// Implementation.
	registryFilename string
	mu               sync.Mutex
	currProto        *enginepb.FileRegistry // guarded by mu
}

const (
	kFileRegistryFilename = "COCKROACHDB_REGISTRY"
)

func (r *FileRegistry) Load() error {
	r.currProto = &enginepb.FileRegistry{}
	r.registryFilename = r.FS.PathJoin(r.DBDir, kFileRegistryFilename)
	f, err := r.FS.Open(r.registryFilename)
	if os.IsNotExist(err) {
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
	if err = r.currProto.Unmarshal(b); err != nil {
		return err
	}
	return nil
}

func (r *FileRegistry) GetFileEntry(filename string) *enginepb.FileEntry {
	filename = r.tryMakeRelativePath(filename)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currProto.Files[filename]
}

// Sets filename => entry and persists the registry.
func (r *FileRegistry) SetFileEntry(filename string, entry *enginepb.FileEntry) error {
	filename = r.tryMakeRelativePath(filename)
	newProto := &enginepb.FileRegistry{}

	r.mu.Lock()
	defer r.mu.Unlock()
	proto.Merge(newProto, r.currProto)
	if newProto.Files == nil {
		newProto.Files = make(map[string]*enginepb.FileEntry)
	}
	newProto.Files[filename] = entry
	return r.writeRegistry(newProto)
}

// Deletes the entry for filename, if it exists, and persists the registry if changed.
func (r *FileRegistry) EnsureDeleteEntry(filename string) error {
	filename = r.tryMakeRelativePath(filename)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currProto.Files[filename] == nil {
		return nil
	}
	newProto := &enginepb.FileRegistry{}
	proto.Merge(newProto, r.currProto)
	delete(newProto.Files, filename)
	return r.writeRegistry(newProto)
}

// Moves the entry under src to dst, if src exists. If src does not exist, but dst exists,
// dst is deleted. Persists the registry if changed.
func (r *FileRegistry) EnsureRenameEntry(src, dst string) error {
	src = r.tryMakeRelativePath(src)
	dst = r.tryMakeRelativePath(dst)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currProto.Files[src] == nil && r.currProto.Files[dst] == nil {
		return nil
	}
	newProto := &enginepb.FileRegistry{}
	proto.Merge(newProto, r.currProto)
	if newProto.Files[src] == nil {
		delete(newProto.Files, dst)
	} else {
		newProto.Files[dst] = newProto.Files[src]
		delete(newProto.Files, src)
	}
	return r.writeRegistry(newProto)
}

// Copies the entry under src to dst, if src exists. If src does not exist, but dst exists,
// dst is deleted. Persists the registry if changed.
func (r *FileRegistry) EnsureLinkEntry(src, dst string) error {
	src = r.tryMakeRelativePath(src)
	dst = r.tryMakeRelativePath(dst)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currProto.Files[src] == nil && r.currProto.Files[dst] == nil {
		return nil
	}
	newProto := &enginepb.FileRegistry{}
	proto.Merge(newProto, r.currProto)
	if newProto.Files[src] == nil {
		delete(newProto.Files, dst)
	} else {
		newProto.Files[dst] = newProto.Files[src]
	}
	return r.writeRegistry(newProto)
}

func (r *FileRegistry) tryMakeRelativePath(filename string) string {
	// Logic copied from file_registry.cc.
	dbDir := r.DBDir
	if len(dbDir) > 0 && dbDir[len(dbDir)-1] != '/' {
		dbDir = dbDir + "/"
	}
	if !strings.HasPrefix(filename, dbDir) {
		return filename
	}
	filename = filename[len(dbDir):]
	if len(filename) > 0 && filename[0] == '/' {
		filename = filename[1:]
	}
	return filename
}

func (r *FileRegistry) writeRegistry(newProto *enginepb.FileRegistry) error {
	if r.ReadOnly {
		return fmt.Errorf("cannot write file registry since db is read-only")
	}
	b, err := newProto.Marshal()
	if err != nil {
		return err
	}
	if err = SafeWriteToFile(r.FS, r.DBDir, r.registryFilename, b); err != nil {
		return err
	}
	r.currProto = newProto
	return nil
}
