// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/baseccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
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
// For query execution spilling to disk: we want to use encryption, but the registries do not need
// to be on disk since on restart the query path will wipe all the existing files it has written.
// The setup would include a memFS and there would logically be four FSs: base-FS (unencrypted
// disk based FS), mem-FS (unencrypted memory based FS), store-FS (encrypted memory based FS),
// data-FS (encrypted disk based FS).
// - The file registry uses mem-FS to store the FileRegistry proto.
// - The StoreKeyManager uses the base-FS to read the user-specified store keys.
// - The store-FS wraps a mem-FS for reading/writing data keys.
// - The DataKeyManager uses the store-FS.
// - The data-FS wraps a base-FS for reading/writing data.

// encryptedFile implements vfs.File.
type encryptedFile struct {
	vfs.File
	mu struct {
		syncutil.Mutex
		rOffset int64
		wOffset int64
	}
	stream FileStream
}

// Write implements io.Writer.
func (f *encryptedFile) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stream.Encrypt(f.mu.wOffset, p)
	n, err = f.File.Write(p)
	f.mu.wOffset += int64(n)
	return n, err
}

// Read implements io.Reader.
func (f *encryptedFile) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err = f.ReadAt(p, f.mu.rOffset)
	f.mu.rOffset += int64(n)
	return n, err
}

// ReadAt implements io.ReaderAt
func (f *encryptedFile) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = f.File.ReadAt(p, off)
	if n > 0 {
		f.stream.Decrypt(off, p[:n])
	}
	return n, err
}

// encryptedFS implements vfs.FS.
type encryptedFS struct {
	vfs.FS
	fileRegistry  *storage.PebbleFileRegistry
	streamCreator *FileCipherStreamCreator
}

// Create implements vfs.FS.Create.
func (fs *encryptedFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return f, err
	}
	settings, stream, err := fs.streamCreator.CreateNew(context.TODO())
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
	ef := &encryptedFile{File: f, stream: stream}
	return vfs.WithFd(f, ef), nil
}

// Link implements vfs.FS.Link.
func (fs *encryptedFS) Link(oldname, newname string) error {
	if err := fs.FS.Link(oldname, newname); err != nil {
		return err
	}
	return fs.fileRegistry.MaybeLinkEntry(oldname, newname)
}

// Open implements vfs.FS.Open.
func (fs *encryptedFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
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
	ef := &encryptedFile{File: f, stream: stream}
	return vfs.WithFd(f, ef), nil
}

// Remove implements vfs.FS.Remove.
func (fs *encryptedFS) Remove(name string) error {
	if err := fs.FS.Remove(name); err != nil {
		return err
	}
	return fs.fileRegistry.MaybeDeleteEntry(name)
}

// Rename implements vfs.FS.Rename.
func (fs *encryptedFS) Rename(oldname, newname string) error {
	if err := fs.FS.Rename(oldname, newname); err != nil {
		return err
	}
	return fs.fileRegistry.MaybeRenameEntry(oldname, newname)
}

// ReuseForWrite implements vfs.FS.ReuseForWrite.
//
// We cannot change any of the key/iv/nonce and reuse the same file since RocksDB does not
// like non-empty WAL files with zero readable entries. There is a todo in env_encryption.cc
// to change this RocksDB behavior. We need to handle a user switching from Pebble to RocksDB,
// so cannot generate WAL files that RocksDB will complain about.
func (fs *encryptedFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	// This is slower than simply calling Create(newname) since the Remove() and Create()
	// will write and sync the file registry file twice. We can optimize this if needed.
	if err := fs.Remove(oldname); err != nil {
		return nil, err
	}
	return fs.Create(newname)
}

type encryptionStatsHandler struct {
	storeKM *StoreKeyManager
	dataKM  *DataKeyManager
}

func (e *encryptionStatsHandler) GetEncryptionStatus() ([]byte, error) {
	var s enginepbccl.EncryptionStatus
	if e.storeKM.activeKey != nil {
		s.ActiveStoreKey = e.storeKM.activeKey.Info
	}
	k, err := e.dataKM.ActiveKey(context.TODO())
	if err != nil {
		return nil, err
	}
	if k != nil {
		s.ActiveDataKey = k.Info
	}
	return protoutil.Marshal(&s)
}

func (e *encryptionStatsHandler) GetDataKeysRegistry() ([]byte, error) {
	r := e.dataKM.getScrubbedRegistry()
	return protoutil.Marshal(r)
}

func (e *encryptionStatsHandler) GetActiveDataKeyID() (string, error) {
	k, err := e.dataKM.ActiveKey(context.TODO())
	if err != nil {
		return "", err
	}
	if k != nil {
		return k.Info.KeyId, nil
	}
	return "plain", nil
}

func (e *encryptionStatsHandler) GetActiveStoreKeyType() int32 {
	if e.storeKM.activeKey != nil {
		return int32(e.storeKM.activeKey.Info.EncryptionType)
	}
	return int32(enginepbccl.EncryptionType_Plaintext)
}

func (e *encryptionStatsHandler) GetKeyIDFromSettings(settings []byte) (string, error) {
	var s enginepbccl.EncryptionSettings
	if err := protoutil.Unmarshal(settings, &s); err != nil {
		return "", err
	}
	return s.KeyId, nil
}

// Init initializes engine.NewEncryptedEncFunc.
func init() {
	storage.NewEncryptedEnvFunc = newEncryptedEnv
}

// newEncryptedEnv creates an encrypted environment and returns the vfs.FS to use for reading and
// writing data. The optionBytes is a binary serialized baseccl.EncryptionOptions, so that non-CCL
// code does not depend on CCL code.
//
// See the comment at the top of this file for the structure of this environment.
func newEncryptedEnv(
	fs vfs.FS, fr *storage.PebbleFileRegistry, dbDir string, readOnly bool, optionBytes []byte,
) (vfs.FS, storage.EncryptionStatsHandler, error) {
	options := &baseccl.EncryptionOptions{}
	if err := protoutil.Unmarshal(optionBytes, options); err != nil {
		return nil, nil, err
	}
	if options.KeySource != baseccl.EncryptionKeySource_KeyFiles {
		return nil, nil, fmt.Errorf("unknown encryption key source: %d", options.KeySource)
	}
	storeKeyManager := &StoreKeyManager{
		fs:                fs,
		activeKeyFilename: options.KeyFiles.CurrentKey,
		oldKeyFilename:    options.KeyFiles.OldKey,
	}
	if err := storeKeyManager.Load(context.TODO()); err != nil {
		return nil, nil, err
	}
	storeFS := &encryptedFS{
		FS:           fs,
		fileRegistry: fr,
		streamCreator: &FileCipherStreamCreator{
			envType:    enginepb.EnvType_Store,
			keyManager: storeKeyManager,
		},
	}
	dataKeyManager := &DataKeyManager{
		fs:             storeFS,
		dbDir:          dbDir,
		rotationPeriod: options.DataKeyRotationPeriod,
	}
	if err := dataKeyManager.Load(context.TODO()); err != nil {
		return nil, nil, err
	}
	dataFS := &encryptedFS{
		FS:           fs,
		fileRegistry: fr,
		streamCreator: &FileCipherStreamCreator{
			envType:    enginepb.EnvType_Data,
			keyManager: dataKeyManager,
		},
	}

	if !readOnly {
		key, err := storeKeyManager.ActiveKey(context.TODO())
		if err != nil {
			return nil, nil, err
		}
		if err := dataKeyManager.SetActiveStoreKeyInfo(context.TODO(), key.Info); err != nil {
			return nil, nil, err
		}
	}
	return dataFS, &encryptionStatsHandler{storeKM: storeKeyManager, dataKM: dataKeyManager}, nil
}
