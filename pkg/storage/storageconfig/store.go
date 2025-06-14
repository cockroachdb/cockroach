// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageconfig

// Store contains the configuration for a store.
type Store struct {
	Path        string
	Size        Size
	BallastSize *Size
	InMemory    bool
	Attributes  []string
	// StickyVFSID is a unique identifier associated with a given store which
	// will preserve the in-memory virtual file system (VFS) even after the
	// storage engine has been closed. This only applies to in-memory storage
	// engine.
	StickyVFSID string
	// PebbleOptions contains Pebble-specific options in the same format as a
	// Pebble OPTIONS file. For example:
	// [Options]
	// delete_range_flush_delay=2s
	// flush_split_bytes=4096
	PebbleOptions string
	// EncryptionOptions is set if encryption is enabled.
	EncryptionOptions *EncryptionOptions
	// ProvisionedRate is optional.
	ProvisionedRate ProvisionedRate
}

func (s *Store) IsEncrypted() bool {
	return s.EncryptionOptions != nil
}

// ProvisionedRate defines the provisioned rate for a store.
type ProvisionedRate struct {
	// ProvisionedBandwidth is the bandwidth provisioned for a store in bytes/s.
	ProvisionedBandwidth int64
	// In the future, we may add more fields here for IOPS or separate read and
	// write bandwidth.
}
