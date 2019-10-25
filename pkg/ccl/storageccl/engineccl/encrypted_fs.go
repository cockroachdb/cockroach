// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/pebble/vfs"
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
//
// For query path spilling to disk: we want to use encryption, but the registries do not need
// to be on disk since on restart the query path will wipe all the existing files it has written.
// The setup would include a memFS and there would logically be four FSs: base-FS (unencrypted
// disk based FS), mem-FS (unencrypted memory based FS), store-FS (encrypted memory based FS),
// data-FS (encrypted disk based FS).
// - The file registry uses mem-FS to store the FileRegistry proto.
// - The StoreKeyManager uses the base-FS to read the user-specified store keys.
// - The store-FS wraps a mem-FS for reading/writing data keys.
// - The DataKeyManager uses the store-FS.
// - The data-FS wraps a base-FS for reading/writing data.

// EncryptedFile implements vfs.File.
type EncryptedFile struct {
	file    vfs.File
	mu struct {
		syncutil.Mutex
		rOffset int64
		wOffset int64
	}
	stream  FileStream
}

// Close implements io.Closer.
func (f *EncryptedFile) Close() error {
	return f.file.Close()
}

// Write implements io.Writer.
func (f *EncryptedFile) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stream.Encrypt(f.mu.wOffset, p)
	n, err = f.file.Write(p)
	f.mu.wOffset += int64(n)
	return n, err
}

// Read implements io.Reader.
func (f *EncryptedFile) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err = f.file.ReadAt(p, f.mu.rOffset)
	if n > 0 {
		f.stream.Decrypt(f.mu.rOffset, p[:n])
	}
	f.mu.rOffset += int64(n)
	return n, err
}

// ReadAt implements io.ReaderAt
func (f *EncryptedFile) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = f.file.ReadAt(p, off)
	if n > 0 {
		f.stream.Decrypt(off, p[:n])
	}
	return n, err
}

// Stat implements vfs.File.Stat.
func (f *EncryptedFile) Stat() (os.FileInfo, error) {
	return f.file.Stat()
}

// Sync implements vfs.File.Sync.
func (f *EncryptedFile) Sync() error {
	return f.file.Sync()
}

// EncryptedFS implements vfs.FS.
type EncryptedFS struct {
	fs            vfs.FS
	fileRegistry  *engine.PebbleFileRegistry
	streamCreator *FileCipherStreamCreator
}

// Create implements vfs.FS.Create. It must not be used for WAL reuse -- see ReuseWAL().
func (fs *EncryptedFS) Create(name string) (vfs.File, error) {
	f, err := fs.fs.Create(name)
	if err != nil {
		return f, err
	}
	settings, stream, err := fs.streamCreator.CreateNew()
	if err != nil {
		f.Close()
		return nil, err
	}
	fproto := &enginepb.FileEntry{}
	fproto.EnvType = fs.streamCreator.envType
	if fproto.EncryptionSettings, err = protoutil.Marshal(settings); err != nil {
		f.Close()
		return nil, err
	}
	if err = fs.fileRegistry.SetFileEntry(name, fproto); err != nil {
		f.Close()
		return nil, err
	}
	return &EncryptedFile{file: f, stream: stream}, nil
}

// Link implements vfs.FS.Link.
func (fs *EncryptedFS) Link(oldname, newname string) error {
	if err := fs.fs.Link(oldname, newname); err != nil {
		return err
	}
	return fs.fileRegistry.MaybeLinkEntry(oldname, newname)
}

// Open implements vfs.FS.Open.
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
			return nil, fmt.Errorf("filename: %s has env %d not equal to FS env %d",
				name, fileEntry.EnvType, fs.streamCreator.envType)
		}
		settings = &enginepbccl.EncryptionSettings{}
		if err := protoutil.Unmarshal(fileEntry.EncryptionSettings, settings); err != nil {
			f.Close()
			return nil, err
		}
	}
	stream, err := fs.streamCreator.CreateExisting(settings)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &EncryptedFile{file: f, stream: stream}, nil
}

// Remove implements vfs.FS.Remove.
func (fs *EncryptedFS) Remove(name string) error {
	if err := fs.fs.Remove(name); err != nil {
		return err
	}
	return fs.fileRegistry.MaybeDeleteEntry(name)
}

// Rename implements vfs.FS.Rename. It must not be used for WAL reuse -- see ReuseWAL().
func (fs *EncryptedFS) Rename(oldname, newname string) error {
	if err := fs.fs.Rename(oldname, newname); err != nil {
		return err
	}
	return fs.fileRegistry.MaybeRenameEntry(oldname, newname)
}

// ReuseWAL attempts to reuse the WAL file with oldname by renaming it to newname and opening
// it for writing.
//
// We cannot change any of the key/iv/nonce and reuse the same file since RocksDB does not
// like non-empty WAL files with zero readable entries. There is a todo in env_encryption.cc
// to change this RocksDB behavior.
//
// TODO(sbhola): add this to Pebble's vfs.FS interface and change WAL reuse to use it.
func (fs *EncryptedFS) ReuseWAL(oldname, newname string) (vfs.File, error) {
	return nil, fmt.Errorf("cannot reuse an encrypted WAL file")
}

// Everything below just forwards to the underlying fs.

// OpenDir implements vfs.FS.OpenDir.
func (fs *EncryptedFS) OpenDir(name string) (vfs.File, error) {
	return fs.fs.OpenDir(name)
}

// MkdirAll implements vfs.FS.MkdirAll.
func (fs *EncryptedFS) MkdirAll(dir string, perm os.FileMode) error {
	return fs.fs.MkdirAll(dir, perm)
}

// Lock implements vfs.FS.Lock.
func (fs *EncryptedFS) Lock(name string) (io.Closer, error) {
	return fs.fs.Lock(name)
}

// List implements vfs.FS.List.
func (fs *EncryptedFS) List(dir string) ([]string, error) {
	return fs.fs.List(dir)
}

// Stat implements vfs.FS.Stat.
func (fs *EncryptedFS) Stat(name string) (os.FileInfo, error) {
	return fs.fs.Stat(name)
}

// PathBase implements vfs.FS.PathBase.
func (fs *EncryptedFS) PathBase(path string) string {
	return fs.fs.PathBase(path)
}

// PathJoin implements vfs.FS.PathJoin.
func (fs *EncryptedFS) PathJoin(elem ...string) string {
	return fs.fs.PathJoin(elem...)
}

// PathDir implements vfs.FS.PathDir.
func (fs *EncryptedFS) PathDir(path string) string {
	return fs.fs.PathDir(path)
}
