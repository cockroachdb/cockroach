// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageconfig

import "time"

// EncryptionOptions defines the per-store encryption options.
type EncryptionOptions struct {
	// The store key source. Defines which fields are useful.
	KeySource EncryptionKeySource
	// Set if key_source == KeyFiles.
	KeyFiles *EncryptionKeyFiles
	// Data key rotation period.
	RotationPeriod time.Duration
}

// EncryptionKeyFiles is used when plain key files are passed.
type EncryptionKeyFiles struct {
	CurrentKey string
	OldKey     string
}

// EncryptionKeySource is an enum identifying the source of the encryption key.
type EncryptionKeySource int32

const (
	EncryptionKeyFromFiles EncryptionKeySource = 0
)

// DefaultRotationPeriod is the rotation period used if not specified.
const DefaultRotationPeriod = time.Hour * 24 * 7 // 1 week, give or take time changes.
