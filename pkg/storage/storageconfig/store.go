// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageconfig

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/errors"
	"go.yaml.in/yaml/v4"
)

// Store contains the configuration for a store.
type Store struct {
	// Type of store (local on-disk, in-memory, or basalt). The default type is
	// local on-disk.
	Type StoreType `yaml:"type,omitempty"`
	// Path of the store. On disk stores must have a path; in-memory stores must
	// not have a path.
	Path string `yaml:"path,omitempty"`
	// Size of the store. If it is a percentage, it is relative to the total size
	// of the output filesystem (or system RAM for in-memory stores). If it is
	// unset, the store will not have a size limit.
	Size Size `yaml:"size,omitempty"`
	// BallastSize contains the size of the ballast file. If it is unset, the size
	// defaults to 1% or 1GiB (whichever is smaller).
	//
	// In-memory stores cannot have a ballast.
	BallastSize Size     `yaml:"ballast-size,omitempty"`
	Attributes  []string `yaml:"attrs,omitempty,flow"`
	// StickyVFSID is a unique identifier associated with a given store which
	// will preserve the in-memory virtual file system (VFS) even after the
	// storage engine has been closed. This only applies to in-memory storage
	// engine.
	StickyVFSID string `yaml:"sticky-vfs-id,omitempty"`
	// PebbleOptions contains Pebble-specific options in the same format as a
	// Pebble OPTIONS file. For example:
	// [Options]
	// delete_range_flush_delay=2s
	// flush_split_bytes=4096
	PebbleOptions string `yaml:"pebble,omitempty"`
	// ProvisionedRate is optional.
	ProvisionedRate ProvisionedRate `yaml:"provisioned-rate,omitempty"`
	// EncryptionOptions is set if encryption is enabled.
	EncryptionOptions *EncryptionOptions `yaml:"encryption,omitempty"`
}

func (s *Store) IsEncrypted() bool {
	return s.EncryptionOptions != nil
}

// IsInMemory returns true if this is an in-memory store.
func (s *Store) IsInMemory() bool { return s.Type == StoreTypeInMemory }

// IsLocal returns true if this is a local on-disk store.
func (s *Store) IsLocal() bool { return s.Type == StoreTypeLocal }

// IsBasalt returns true if this is a basalt object storage store.
func (s *Store) IsBasalt() bool { return s.Type == StoreTypeBasalt }

// MinimumStoreSize is the smallest size in bytes that a store can have. This
// number is based on config's defaultZoneConfig's RangeMaxBytes, which is
// extremely stable. To avoid adding the dependency on config here, it is just
// hard coded to 640MiB.
const MinimumStoreSize = 10 * 64 << 20

// BasaltPathPrefix is the prefix of a path used when a store is configured with
// basalt.
const BasaltPathPrefix = "basalt://"

// Validate checks all fields, possibly normalizing them and filling in
// defaults.
func (s *Store) Validate() error {
	if s.Size.IsBytes() && s.Size.Bytes() < MinimumStoreSize {
		return errors.Newf("store size (%s) must be at least %s",
			s.Size, crhumanize.Bytes(MinimumStoreSize))
	}
	if s.Size.IsPercent() && s.Size.Percent() < 1 {
		return errors.Newf("store size (%s) must be at least 1%%", s.Size)
	}
	if s.BallastSize.IsPercent() && s.BallastSize.Percent() > 50 {
		return errors.Newf("ballast size (%s) must be at most 50%%", s.BallastSize)
	}
	switch s.Type {
	case StoreTypeInMemory:
		// In-memory stores don't use a path and require a size.
		if s.Path != "" {
			return errors.Newf("path specified for in-memory store")
		}
		if !s.Size.IsSet() {
			return errors.Newf("size must be specified for in-memory store")
		}
		if s.BallastSize.IsSet() {
			return errors.Newf("ballast-size specified for in-memory store")
		}
	case StoreTypeLocal:
		// Local on-disk stores must have a path.
		if s.Path == "" {
			return errors.Newf("no path specified for local on-disk store")
		}
		if s.StickyVFSID != "" {
			return errors.Newf("local on-disk store cannot use sticky VFS ID")
		}
	case StoreTypeBasalt:
		// Basalt stores must have a path with "basalt://" prefix and must not
		// have encryption, ballast size, and VFS ID set.
		if s.Path == "" {
			return errors.Newf("no path specified for basalt store")
		}
		if !strings.HasPrefix(s.Path, BasaltPathPrefix) {
			return errors.Newf("basalt store path must start with %s", BasaltPathPrefix)
		}
		if s.EncryptionOptions != nil {
			return errors.Newf("encryption is not supported for basalt stores")
		}
		if s.BallastSize.IsSet() {
			return errors.Newf("ballast-size specified for basalt store")
		}
		if s.StickyVFSID != "" {
			return errors.Newf("basalt store cannot use sticky VFS ID")
		}
	}
	if s.EncryptionOptions != nil {
		if err := s.EncryptionOptions.Validate(); err != nil {
			return errors.Wrap(err, "invalid encryption options")
		}
	}

	return nil
}

// StoreType identifies the type of a store.
type StoreType int

const (
	// StoreTypeLocal is a local on-disk store (the default/zero value).
	StoreTypeLocal StoreType = iota
	// StoreTypeInMemory is an in-memory store.
	StoreTypeInMemory
	// StoreTypeBasalt is a basalt object storage store.
	StoreTypeBasalt
)

var _ yaml.IsZeroer = StoreType(0)
var _ yaml.Marshaler = StoreType(0)
var _ yaml.Unmarshaler = (*StoreType)(nil)

func (StoreType) SafeValue() {}
func (s StoreType) String() string {
	switch s {
	case StoreTypeLocal:
		return "local"
	case StoreTypeInMemory:
		return "mem"
	case StoreTypeBasalt:
		return "basalt"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

func (s StoreType) IsZero() bool {
	// Local (the default) is the zero value.
	return s == StoreTypeLocal
}

func (s StoreType) MarshalYAML() (any, error) {
	switch s {
	case StoreTypeLocal:
		return "", nil
	case StoreTypeInMemory:
		return "mem", nil
	case StoreTypeBasalt:
		return "basalt", nil
	default:
		return nil, errors.Newf("unknown store type: %d", int(s))
	}
}

func (s *StoreType) UnmarshalYAML(value *yaml.Node) error {
	switch {
	case value.Value == "" || strings.EqualFold(value.Value, "local"):
		*s = StoreTypeLocal
	case strings.EqualFold(value.Value, "mem"):
		*s = StoreTypeInMemory
	case strings.EqualFold(value.Value, "basalt"):
		*s = StoreTypeBasalt
	default:
		return errors.Newf("unknown store type: %q", value.Value)
	}
	return nil
}

// ProvisionedRate defines the provisioned rate for a store.
type ProvisionedRate struct {
	// ProvisionedBandwidth is the bandwidth provisioned for a store in bytes/s.
	ProvisionedBandwidth int64
	// In the future, we may add more fields here for IOPS or separate read and
	// write bandwidth.
}

func (pr ProvisionedRate) String() string {
	return "bandwidth=" + string(crhumanize.BytesPerSec(pr.ProvisionedBandwidth, crhumanize.Exact, crhumanize.Compact))
}

var _ yaml.Marshaler = ProvisionedRate{}
var _ yaml.Unmarshaler = (*ProvisionedRate)(nil)

func (pr ProvisionedRate) IsZero() bool {
	return pr == (ProvisionedRate{})
}

func (pr ProvisionedRate) MarshalYAML() (any, error) {
	return pr.String(), nil
}

func (pr *ProvisionedRate) UnmarshalYAML(value *yaml.Node) error {
	res, err := ParseProvisionedRate(value.Value)
	if err != nil {
		return err
	}
	*pr = res
	return nil
}

func ParseProvisionedRate(value string) (ProvisionedRate, error) {
	if value == "" {
		return ProvisionedRate{}, nil
	}
	split := strings.Split(value, "=")
	if len(split) != 2 {
		return ProvisionedRate{}, errors.Errorf("provisioned-rate has invalid value %q", value)
	}
	subField, subValue := split[0], split[1]
	if subField != "bandwidth" {
		return ProvisionedRate{}, errors.Errorf("provisioned-rate does not have bandwidth sub-field")
	}
	bps, err := crhumanize.ParseBytesPerSec[int64](subValue)
	if err != nil || bps == 0 {
		return ProvisionedRate{}, errors.Errorf("provisioned-rate has invalid bandwidth value %q", subValue)
	}
	return ProvisionedRate{ProvisionedBandwidth: bps}, nil
}
