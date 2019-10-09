package engineccl

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/pebble/vfs"
	"io"
	"os"
	"sync"
)

// High-level code structure.
//
// A pebble instance can operate in two modes:
// - With a single FS, used for all files.
// - With three FSs, when encryption-at-rest is on. We refer to these FSs as the base-FS,
//   store-FS and data-FS. The base-FS contains unencrypted files, the store-FS contains
//   files encrypted using the user-specified store keys, and the data-FS contains files
//   encrypted using generated keys.
//
// Both the data-FS and store-FS are wrappers around the base-FS, i.e., they encrypt and
// decrypt data being written to and read from files in the base-FS. The environment in which
// these exist knows which file to open in which FS. Specifically,
// - The file registry uses the base-FS to store the FileRegistry proto. This registry provides
//   information about files in the store-FS and data-FS (the EnvType::Store and EnvType::Data
//   respectively). This information includes which FS the file belongs to (for consistency
//   checking that a file created using one FS is not being read in another) and information
//   about encryption settings used for the file, including the key id.
// - The StoreKeyManager uses the base-FS to read the user-specified store keys at startup.
//   These are in two key files: the active key file and the old key file, which contain the
//   key id and the key.
// - The store-FS is used only for storing the key file for the generated keys. It is used by
//   the DataKeyManager. These keys are rotated periodically in a simple manner -- a new
//   active key is generated for future file writes. Existing files are not affected.
//
// The data-FS and store-FS both use a common implementation. They consume:
// - the FS they are wrapping: it is always the base-FS in our case, but it does not matter.
// - The single file registry they share to record information about their files.
// - A KeyManager interface wrapped in a FileStreamCreator, that provides a simple
//   interface for encrypting and decrypting data in a file at arbitrary byte offsets.
//
// Both data-FS and store-FS can be operating in plaintext mode if the user-specified store
// keys are "plain".

// Implements vfs.File.
type EncryptedFile struct {
	file    vfs.File
	mu      sync.Mutex
	rOffset int64 // guarded by mu
	wOffset int64 // guarded by mu
	stream  FileStream
}

func (f *EncryptedFile) Close() error {
	return f.file.Close()
}

func (f *EncryptedFile) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stream.Encrypt(f.wOffset, p)
	n, err = f.Write(p)
	f.wOffset += int64(n)
	return n, err
}

func (f *EncryptedFile) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err = f.file.ReadAt(p, f.rOffset)
	if n > 0 {
		f.stream.Decrypt(f.rOffset, p[:n])
	}
	f.rOffset += int64(n)
	return n, err
}

func (f *EncryptedFile) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = f.file.ReadAt(p, off)
	if n > 0 {
		f.stream.Decrypt(off, p[:n])
	}
	return n, err
}

func (f *EncryptedFile) Stat() (os.FileInfo, error) {
	return f.file.Stat()
}

func (f *EncryptedFile) Sync() error {
	return f.file.Sync()
}

// TODO: should I bother with errorFile or just return nil?
type errorFile struct{}

func (f errorFile) Close() error {
	return nil
}
func (f errorFile) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("write not allowed on errorFile")
}
func (f errorFile) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read not allowed on errorFile")
}
func (f errorFile) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, fmt.Errorf("read not allowed on errorFile")
}
func (f errorFile) Stat() (os.FileInfo, error) {
	return nil, fmt.Errorf("stat not allowed on errorFile")
}
func (f errorFile) Sync() error {
	return fmt.Errorf("sync not allowed on errorFile")
}

// Implements vfs.FS
type EncryptedFS struct {
	fs            vfs.FS
	fileRegistry  *engine.FileRegistry
	streamCreator *FileCipherStreamCreator
}

// Create creates the named file for writing, truncating it if it already exists.
func (fs *EncryptedFS) Create(name string) (vfs.File, error) {
	f, err := fs.fs.Create(name)
	if err != nil {
		return f, err
	}
	settings, stream, err := fs.streamCreator.CreateNew()
	if err != nil {
		f.Close()
		return errorFile{}, err
	}
	fproto := &enginepb.FileEntry{}
	fproto.EnvType = fs.streamCreator.envType
	if fproto.EncryptionSettings, err = settings.Marshal(); err != nil {
		f.Close()
		return errorFile{}, err
	}
	if err = fs.fileRegistry.SetFileEntry(name, fproto); err != nil {
		f.Close()
		return errorFile{}, err
	}
	return &EncryptedFile{file: f, stream: stream}, nil
}

// Link creates newname as a hard link to the oldname file.
func (fs *EncryptedFS) Link(oldname, newname string) error {
	if err := fs.fs.Link(oldname, newname); err != nil {
		return err
	}
	return fs.fileRegistry.EnsureLinkEntry(oldname, newname)
}

// Open opens the named file for reading.
func (fs *EncryptedFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.fs.Open(name, opts...)
	if err != nil {
		return f, err
	}
	fileEntry := fs.fileRegistry.GetFileEntry(name)
	var settings *enginepbccl.EncryptionSettings
	if fileEntry != nil {
		if fileEntry.EnvType != fs.streamCreator.envType {
			f.Close()
			return errorFile{}, fmt.Errorf("filename: %s has env %d not equal to FS env %d",
				name, fileEntry.EnvType, fs.streamCreator.envType)
		}
		settings = &enginepbccl.EncryptionSettings{}
		if err := settings.Unmarshal(fileEntry.EncryptionSettings); err != nil {
			f.Close()
			return errorFile{}, err
		}
	}
	stream, err := fs.streamCreator.CreateExisting(settings)
	if err != nil {
		f.Close()
		return errorFile{}, err
	}
	return &EncryptedFile{file: f, stream: stream}, nil
}

// OpenDir opens the named directory for syncing.
func (fs *EncryptedFS) OpenDir(name string) (vfs.File, error) {
	return fs.fs.OpenDir(name)
}

// Remove removes the named file or directory.
func (fs *EncryptedFS) Remove(name string) error {
	if err := fs.fs.Remove(name); err != nil {
		return err
	}
	return fs.fileRegistry.EnsureDeleteEntry(name)
}

// Rename renames a file. It overwrites the file at newname if one exists,
// the same as os.Rename.
//
// TODO: this function is used both for creating and immediately renaming (e.g. MANIFEST files),
// for which the following code is fine, and for recycling WAL files. We should split
// into two different functions.
// For the latter we do not want to continue with the same key if the active key has changed. The current
// code in env_encryption.cc does not try to reuse in this case at all (find out typical
// key rotation settings and how that compares to WAL file recycling). If we change the
// key and use the same underlying file it does not work due to the possibility of zero
// readable entries (there is a TODO(ajkr) in env_encryption.cc about this).
func (fs *EncryptedFS) Rename(oldname, newname string) error {
	if err := fs.fs.Link(oldname, newname); err != nil {
		return err
	}
	return fs.fileRegistry.EnsureRenameEntry(oldname, newname)
}

func (fs *EncryptedFS) MkdirAll(dir string, perm os.FileMode) error {
	return fs.fs.MkdirAll(dir, perm)
}

func (fs *EncryptedFS) Lock(name string) (io.Closer, error) {
	return fs.fs.Lock(name)
}

func (fs *EncryptedFS) List(dir string) ([]string, error) {
	return fs.fs.List(dir)
}

func (fs *EncryptedFS) Stat(name string) (os.FileInfo, error) {
	return fs.fs.Stat(name)
}

func (fs *EncryptedFS) PathBase(path string) string {
	return fs.fs.PathBase(path)
}

func (fs *EncryptedFS) PathJoin(elem ...string) string {
	return fs.fs.PathJoin(elem...)
}

func (fs *EncryptedFS) PathDir(path string) string {
	return fs.fs.PathDir(path)
}
