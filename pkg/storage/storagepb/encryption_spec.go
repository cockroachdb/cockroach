// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storagepb

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// DefaultRotationPeriod is the rotation period used if not specified.
const DefaultRotationPeriod = time.Hour * 24 * 7 // 1 week, give or take time changes.

// Special value of key paths to mean "no encryption". We do not accept empty fields.
const plaintextFieldValue = "plain"

// StoreEncryptionSpec contains the details that can be specified in the cli via
// the --enterprise-encryption flag.
type StoreEncryptionSpec struct {
	Options EncryptionOptions
	Path    string
}

// String returns a fully parsable version of the encryption spec.
func (es StoreEncryptionSpec) String() string {
	// All fields are set.
	return fmt.Sprintf("path=%s,key=%s,old-key=%s,rotation-period=%s",
		es.Path, es.Options.KeyFiles.CurrentKey, es.Options.KeyFiles.OldKey, es.RotationPeriod(),
	)
}

// RotationPeriod returns the rotation period as a duration.
func (es StoreEncryptionSpec) RotationPeriod() time.Duration {
	return time.Duration(es.Options.DataKeyRotationPeriod) * time.Second
}

// PathMatches returns true if this StoreEncryptionSpec matches the given store path.
func (es StoreEncryptionSpec) PathMatches(path string) bool {
	return es.Path == path || es.Path == "*"
}

// NewStoreEncryptionSpec parses the string passed in and returns a new
// StoreEncryptionSpec if parsing succeeds.
// TODO(mberhault): we should share the parsing code with the StoreSpec.
func NewStoreEncryptionSpec(value string) (StoreEncryptionSpec, error) {
	const pathField = "path"
	es := StoreEncryptionSpec{
		Path: "",
		Options: EncryptionOptions{
			KeySource:             EncryptionKeySource_KeyFiles,
			KeyFiles:              &EncryptionKeyFiles{},
			DataKeyRotationPeriod: int64(DefaultRotationPeriod / time.Second),
		},
	}

	used := make(map[string]struct{})
	for _, split := range strings.Split(value, ",") {
		if len(split) == 0 {
			continue
		}
		subSplits := strings.SplitN(split, "=", 2)
		if len(subSplits) == 1 {
			return StoreEncryptionSpec{}, fmt.Errorf("field not in the form <key>=<value>: %s", split)
		}
		field := strings.ToLower(subSplits[0])
		value := subSplits[1]
		if _, ok := used[field]; ok {
			return StoreEncryptionSpec{}, fmt.Errorf("%s field was used twice in encryption definition", field)
		}
		used[field] = struct{}{}

		if len(field) == 0 {
			return StoreEncryptionSpec{}, fmt.Errorf("empty field")
		}
		if len(value) == 0 {
			return StoreEncryptionSpec{}, fmt.Errorf("no value specified for %s", field)
		}

		switch field {
		case pathField:
			if value == "*" {
				es.Path = value
			} else {
				var err error
				es.Path, err = GetAbsoluteFSPath(pathField, value)
				if err != nil {
					return StoreEncryptionSpec{}, err
				}
			}
		case "key":
			if value == plaintextFieldValue {
				es.Options.KeyFiles.CurrentKey = plaintextFieldValue
			} else {
				var err error
				es.Options.KeyFiles.CurrentKey, err = GetAbsoluteFSPath("key", value)
				if err != nil {
					return StoreEncryptionSpec{}, err
				}
			}
		case "old-key":
			if value == plaintextFieldValue {
				es.Options.KeyFiles.OldKey = plaintextFieldValue
			} else {
				var err error
				es.Options.KeyFiles.OldKey, err = GetAbsoluteFSPath("old-key", value)
				if err != nil {
					return StoreEncryptionSpec{}, err
				}
			}
		case "rotation-period":
			dur, err := time.ParseDuration(value)
			if err != nil {
				return StoreEncryptionSpec{}, errors.Wrapf(err, "could not parse rotation-duration value: %s", value)
			}
			es.Options.DataKeyRotationPeriod = int64(dur / time.Second)
		default:
			return StoreEncryptionSpec{}, fmt.Errorf("%s is not a valid enterprise-encryption field", field)
		}
	}

	// Check that all fields are set.
	if es.Path == "" {
		return StoreEncryptionSpec{}, fmt.Errorf("no path specified")
	}
	if es.Options.KeyFiles.CurrentKey == "" {
		return StoreEncryptionSpec{}, fmt.Errorf("no key specified")
	}
	if es.Options.KeyFiles.OldKey == "" {
		return StoreEncryptionSpec{}, fmt.Errorf("no old-key specified")
	}

	return es, nil
}

// EncryptionSpecList contains a slice of StoreEncryptionSpecs that implements pflag's value
// interface.
type EncryptionSpecList struct {
	Specs []StoreEncryptionSpec
}

var _ pflag.Value = &EncryptionSpecList{}

// String returns a string representation of all the StoreEncryptionSpecs. This is part
// of pflag's value interface.
func (encl EncryptionSpecList) String() string {
	var buffer bytes.Buffer
	for _, ss := range encl.Specs {
		fmt.Fprintf(&buffer, "--%s=%s ", cliflags.EnterpriseEncryption.Name, ss)
	}
	// Trim the extra space from the end if it exists.
	if l := buffer.Len(); l > 0 {
		buffer.Truncate(l - 1)
	}
	return buffer.String()
}

// Type returns the underlying type in string form. This is part of pflag's
// value interface.
func (encl *EncryptionSpecList) Type() string {
	return "EncryptionSpec"
}

// Set adds a new value to the StoreEncryptionSpecValue. It is the important part of
// pflag's value interface.
func (encl *EncryptionSpecList) Set(value string) error {
	spec, err := NewStoreEncryptionSpec(value)
	if err != nil {
		return err
	}
	if encl.Specs == nil {
		encl.Specs = []StoreEncryptionSpec{spec}
	} else {
		encl.Specs = append(encl.Specs, spec)
	}
	return nil
}

// EncryptionOptionsForStore takes a store directory and returns its EncryptionOptions
// if a matching entry if found in the StoreEncryptionSpecList.
func EncryptionOptionsForStore(
	dir string, encryptionSpecs EncryptionSpecList,
) (*EncryptionOptions, error) {
	// We need an absolute path, but the input may have come in relative.
	path, err := filepath.Abs(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "could not find absolute path for %s ", dir)
	}
	for _, es := range encryptionSpecs.Specs {
		if es.PathMatches(path) {
			return &es.Options, nil
		}
	}
	return nil, nil
}

// GetAbsoluteFSPath takes a (possibly relative) and returns the absolute path.
// Returns an error if the path begins with '~' or Abs fails.
// 'fieldName' is used in error strings.
func GetAbsoluteFSPath(fieldName string, p string) (string, error) {
	if p[0] == '~' {
		return "", fmt.Errorf("%s cannot start with '~': %s", fieldName, p)
	}

	ret, err := filepath.Abs(p)
	if err != nil {
		return "", errors.Wrapf(err, "could not find absolute path for %s %s", fieldName, p)
	}
	return ret, nil
}
