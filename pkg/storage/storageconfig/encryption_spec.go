// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageconfig

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"go.yaml.in/yaml/v4"
)

// EncryptionOptions defines the per-store encryption options.
type EncryptionOptions struct {
	// The store key source. Defines which fields are useful.
	KeySource EncryptionKeySource `yaml:"key-source,omitempty"`
	// Set if KeySource == EncryptionKeyFiles.
	KeyFiles *EncryptionKeyFiles `yaml:",inline"`
	// Data key rotation period. Defaults to 7 days if not specified.
	RotationPeriod time.Duration `yaml:"rotation-period,omitempty"`
}

// EncryptionKeyFiles is used when plain key files are passed.
type EncryptionKeyFiles struct {
	// Path to the current encryption key file, or "plain" for no encryption.
	CurrentKey string `yaml:"key"`
	// Path to the previous encryption key file, or "plain" for no encryption.
	OldKey string `yaml:"old-key"`
}

// EncryptionKeySource is an enum identifying the source of the encryption key.
type EncryptionKeySource int32

const (
	EncryptionKeyFromFiles EncryptionKeySource = 0
)

var _ yaml.IsZeroer = EncryptionKeyFromFiles

func (e EncryptionKeySource) IsZero() bool {
	return e == 0
}

// DefaultRotationPeriod is the rotation period used if not specified.
const DefaultRotationPeriod = time.Hour * 24 * 7 // 1 week, give or take time changes.

// PlaintextKeyValue is the value of a key field that indicates no encryption.
const PlaintextKeyValue = "plain"

// Validate checks all fields, possibly normalizing them and filling in
// defaults.
func (e *EncryptionOptions) Validate() error {
	if e.RotationPeriod == 0 {
		e.RotationPeriod = DefaultRotationPeriod
	} else if e.RotationPeriod < time.Second {
		return errors.Newf("invalid rotation period %s", e.RotationPeriod)
	}
	switch e.KeySource {
	case EncryptionKeyFromFiles:
		if e.KeyFiles == nil {
			return errors.New("no key files specified")
		}
		currentKey, err := getAbsoluteFSPath("key", e.KeyFiles.CurrentKey)
		if err != nil {
			return err
		}
		oldKey, err := getAbsoluteFSPath("old key", e.KeyFiles.OldKey)
		if err != nil {
			return err
		}
		e.KeyFiles.CurrentKey = currentKey
		e.KeyFiles.OldKey = oldKey
		return nil

	default:
		return errors.Newf("unknown key source %d", e.KeySource)
	}
}

// getAbsoluteFSPath takes a (possibly relative) path to an encryption key and
// returns the absolute path. Handles PlaintextKeyValue as a special case.
//
// Returns an error if the path begins with '~' or Abs fails.
// 'fieldName' is used in error strings.
func getAbsoluteFSPath(fieldName string, p string) (string, error) {
	if p == "" {
		return "", errors.Newf("no %s specified", fieldName)
	}
	if p == PlaintextKeyValue {
		return p, nil
	}
	if strings.HasPrefix(p, "~") {
		return "", errors.Newf("%s %q cannot start with '~'", fieldName, p)
	}

	ret, err := filepath.Abs(p)
	if err != nil {
		return "", errors.Wrapf(err, "could not find absolute path for %s %q", fieldName, p)
	}
	return ret, nil
}
