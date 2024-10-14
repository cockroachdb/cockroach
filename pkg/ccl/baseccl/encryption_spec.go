// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package baseccl

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/cliccl/cliflagsccl"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
	Path           string
	KeyPath        string
	OldKeyPath     string
	RotationPeriod time.Duration
}

// ToEncryptionOptions convert to a serialized EncryptionOptions protobuf.
func (es StoreEncryptionSpec) ToEncryptionOptions() ([]byte, error) {
	opts := EncryptionOptions{
		KeySource: EncryptionKeySource_KeyFiles,
		KeyFiles: &EncryptionKeyFiles{
			CurrentKey: es.KeyPath,
			OldKey:     es.OldKeyPath,
		},
		DataKeyRotationPeriod: int64(es.RotationPeriod / time.Second),
	}

	return protoutil.Marshal(&opts)
}

// String returns a fully parsable version of the encryption spec.
func (es StoreEncryptionSpec) String() string {
	// All fields are set.
	return fmt.Sprintf("path=%s,key=%s,old-key=%s,rotation-period=%s",
		es.Path, es.KeyPath, es.OldKeyPath, es.RotationPeriod)
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
	var es StoreEncryptionSpec
	es.RotationPeriod = DefaultRotationPeriod

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
				es.Path, err = base.GetAbsoluteFSPath(pathField, value)
				if err != nil {
					return StoreEncryptionSpec{}, err
				}
			}
		case "key":
			if value == plaintextFieldValue {
				es.KeyPath = plaintextFieldValue
			} else {
				var err error
				es.KeyPath, err = base.GetAbsoluteFSPath("key", value)
				if err != nil {
					return StoreEncryptionSpec{}, err
				}
			}
		case "old-key":
			if value == plaintextFieldValue {
				es.OldKeyPath = plaintextFieldValue
			} else {
				var err error
				es.OldKeyPath, err = base.GetAbsoluteFSPath("old-key", value)
				if err != nil {
					return StoreEncryptionSpec{}, err
				}
			}
		case "rotation-period":
			var err error
			es.RotationPeriod, err = time.ParseDuration(value)
			if err != nil {
				return StoreEncryptionSpec{}, errors.Wrapf(err, "could not parse rotation-duration value: %s", value)
			}
		default:
			return StoreEncryptionSpec{}, fmt.Errorf("%s is not a valid enterprise-encryption field", field)
		}
	}

	// Check that all fields are set.
	if es.Path == "" {
		return StoreEncryptionSpec{}, fmt.Errorf("no path specified")
	}
	if es.KeyPath == "" {
		return StoreEncryptionSpec{}, fmt.Errorf("no key specified")
	}
	if es.OldKeyPath == "" {
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
		fmt.Fprintf(&buffer, "--%s=%s ", cliflagsccl.EnterpriseEncryption.Name, ss)
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

// PopulateWithEncryptionOpts iterates through the EncryptionSpecList and looks
// for matching paths in the StoreSpecList and WAL failover config. Any
// unmatched EncryptionSpec causes an error.
func PopulateWithEncryptionOpts(
	storeSpecs base.StoreSpecList,
	walFailoverConfig *base.WALFailoverConfig,
	encryptionSpecs EncryptionSpecList,
) error {
	for _, es := range encryptionSpecs.Specs {
		var found bool
		for i := range storeSpecs.Specs {
			if !es.PathMatches(storeSpecs.Specs[i].Path) {
				continue
			}

			// Found a matching path.
			if len(storeSpecs.Specs[i].EncryptionOptions) > 0 {
				return fmt.Errorf("store with path %s already has an encryption setting",
					storeSpecs.Specs[i].Path)
			}

			opts, err := es.ToEncryptionOptions()
			if err != nil {
				return err
			}
			storeSpecs.Specs[i].EncryptionOptions = opts
			found = true
			break
		}

		for _, externalPath := range [2]*base.ExternalPath{&walFailoverConfig.Path, &walFailoverConfig.PrevPath} {
			if !externalPath.IsSet() || !es.PathMatches(externalPath.Path) {
				continue
			}
			// NB: The external paths WALFailoverConfig.Path and
			// WALFailoverConfig.PrevPath are only ever set in single-store
			// configurations. In multi-store with among-stores failover mode, these
			// will be empty (so we won't encounter the same path twice).
			if len(externalPath.EncryptionOptions) > 0 {
				return fmt.Errorf("WAL failover path %s already has an encryption setting",
					externalPath.Path)
			}
			opts, err := es.ToEncryptionOptions()
			if err != nil {
				return err
			}
			externalPath.EncryptionOptions = opts
			found = true
		}

		if !found {
			return fmt.Errorf("no usage of path %s found for encryption setting: %v", es.Path, es)
		}
	}
	return nil
}

// EncryptionOptionsForStore takes a store directory and returns its EncryptionOptions
// if a matching entry if found in the StoreEncryptionSpecList.
func EncryptionOptionsForStore(dir string, encryptionSpecs EncryptionSpecList) ([]byte, error) {
	// We need an absolute path, but the input may have come in relative.
	path, err := filepath.Abs(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "could not find absolute path for %s ", dir)
	}
	for _, es := range encryptionSpecs.Specs {
		if es.PathMatches(path) {
			return es.ToEncryptionOptions()
		}
	}
	return nil, nil
}
