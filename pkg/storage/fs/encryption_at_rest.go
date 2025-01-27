// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// NewEncryptedEnvFunc creates an encrypted environment and returns the vfs.FS to use for reading
// and writing data. This should be initialized by calling engineccl.Init() before calling
// NewPebble(). The optionBytes is a binary serialized storagepb.EncryptionOptions.
var NewEncryptedEnvFunc func(
	fs vfs.FS, fr *FileRegistry, dbDir string, readOnly bool, encryptionOptions *storagepb.EncryptionOptions,
) (*EncryptionEnv, error)

// resolveEncryptedEnvOptions creates the EncryptionEnv and associated file
// registry if this store has encryption-at-rest enabled; otherwise returns a
// nil EncryptionEnv.
func resolveEncryptedEnvOptions(
	ctx context.Context,
	unencryptedFS vfs.FS,
	dir string,
	encryptionOpts *storagepb.EncryptionOptions,
	rw RWMode,
) (*FileRegistry, *EncryptionEnv, error) {
	if encryptionOpts == nil {
		// There's no encryption config. This is valid if the user doesn't
		// intend to use encryption-at-rest, and the store has never had
		// encryption-at-rest enabled. Validate that there's no file registry.
		// If there is, the caller is required to specify an
		// --enterprise-encryption flag for this store.
		if err := checkNoRegistryFile(unencryptedFS, dir); err != nil {
			return nil, nil, fmt.Errorf("encryption was used on this store before, but no encryption flags " +
				"specified. You must fully specify the --enterprise-encryption flag")
		}
		return nil, nil, nil
	}

	// We'll need to use the encryption-at-rest filesystem. Even if the store
	// isn't configured to encrypt files, there may still be encrypted files
	// from a previous configuration.
	if NewEncryptedEnvFunc == nil {
		return nil, nil, fmt.Errorf("encryption is enabled but no function to create the encrypted env")
	}
	fileRegistry := &FileRegistry{FS: unencryptedFS, DBDir: dir, ReadOnly: rw == ReadOnly,
		NumOldRegistryFiles: DefaultNumOldFileRegistryFiles}
	if err := fileRegistry.Load(ctx); err != nil {
		return nil, nil, err
	}
	env, err := NewEncryptedEnvFunc(unencryptedFS, fileRegistry, dir, rw == ReadOnly, encryptionOpts)
	if err != nil {
		return nil, nil, errors.WithSecondaryError(err, fileRegistry.Close())
	}
	return fileRegistry, env, nil
}

// EncryptionEnv describes the encryption-at-rest environment, providing
// access to a filesystem with on-the-fly encryption.
type EncryptionEnv struct {
	// Closer closes the encryption-at-rest environment. Once the
	// environment is closed, the environment's VFS may no longer be
	// used.
	Closer io.Closer
	// FS provides the encrypted virtual filesystem. New files are
	// transparently encrypted.
	FS vfs.FS
	// StatsHandler exposes encryption-at-rest state for observability.
	StatsHandler EncryptionStatsHandler
}

// EncryptionRegistries contains the encryption-related registries:
// Both are serialized protobufs.
type EncryptionRegistries struct {
	// FileRegistry is the list of files with encryption status.
	// serialized storage/engine/enginepb/file_registry.proto::FileRegistry
	FileRegistry []byte
	// KeyRegistry is the list of keys, scrubbed of actual key data.
	// serialized storage/enginepb/key_registry.proto::DataKeysRegistry
	KeyRegistry []byte
}

// EncryptionStatsHandler provides encryption related stats.
type EncryptionStatsHandler interface {
	// Returns a serialized enginepb.EncryptionStatus.
	GetEncryptionStatus() ([]byte, error)
	// Returns a serialized enginepb.DataKeysRegistry, scrubbed of key contents.
	GetDataKeysRegistry() ([]byte, error)
	// Returns the ID of the active data key, or "plain" if none.
	GetActiveDataKeyID() (string, error)
	// Returns the enum value of the encryption type.
	GetActiveStoreKeyType() int32
	// Returns the KeyID embedded in the serialized EncryptionSettings.
	GetKeyIDFromSettings(settings []byte) (string, error)
}

// EnvStats is a set of RocksDB env stats, including encryption status.
type EnvStats struct {
	// TotalFiles is the total number of files reported by rocksdb.
	TotalFiles uint64
	// TotalBytes is the total size of files reported by rocksdb.
	TotalBytes uint64
	// ActiveKeyFiles is the number of files using the active data key.
	ActiveKeyFiles uint64
	// ActiveKeyBytes is the size of files using the active data key.
	ActiveKeyBytes uint64
	// EncryptionType is an enum describing the active encryption algorithm.
	// See: storage/engine/enginepb/key_registry.proto
	EncryptionType int32
	// EncryptionStatus is a serialized enginepb/stats.proto::EncryptionStatus protobuf.
	EncryptionStatus []byte
}
