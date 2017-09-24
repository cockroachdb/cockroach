// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package base

import (
	"bytes"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
)

// This file implements method receivers for members of server.Context struct
// -- 'Stores' and 'JoinList', which satisfies pflag's value interface

// MinimumStoreSize is the smallest size in bytes that a store can have. This
// number is based on config's defaultZoneConfig's RangeMaxBytes, which is
// extremely stable. To avoid adding the dependency on config here, it is just
// hard coded to 640MiB.
const MinimumStoreSize = 10 * 64 << 20

// StoreSpec contains the details that can be specified in the cli pertaining
// to the --store flag.
type StoreSpec struct {
	Path string
	// SizeInBytes is used for calculating free space and making rebalancing
	// decisions. Zero indicates that there is no maximum size. This value is not
	// actually used by the engine and thus not enforced.
	SizeInBytes int64
	SizePercent float64
	InMemory    bool
	Attributes  roachpb.Attributes
}

// String returns a fully parsable version of the store spec.
func (ss StoreSpec) String() string {
	var buffer bytes.Buffer
	if len(ss.Path) != 0 {
		fmt.Fprintf(&buffer, "path=%s,", ss.Path)
	}
	if ss.InMemory {
		fmt.Fprint(&buffer, "type=mem,")
	}
	if ss.SizeInBytes > 0 {
		fmt.Fprintf(&buffer, "size=%s,", humanizeutil.IBytes(ss.SizeInBytes))
	}
	if ss.SizePercent > 0 {
		fmt.Fprintf(&buffer, "size=%s%%,", humanize.Ftoa(ss.SizePercent))
	}
	if len(ss.Attributes.Attrs) > 0 {
		fmt.Fprint(&buffer, "attrs=")
		for i, attr := range ss.Attributes.Attrs {
			if i != 0 {
				fmt.Fprint(&buffer, ":")
			}
			fmt.Fprintf(&buffer, attr)
		}
		fmt.Fprintf(&buffer, ",")
	}
	// Trim the extra comma from the end if it exists.
	if l := buffer.Len(); l > 0 {
		buffer.Truncate(l - 1)
	}
	return buffer.String()
}

// fractionRegex is the regular expression that recognizes whether
// the specified size is a fraction of the total available space.
// Proportional sizes can be expressed as fractional numbers, either
// in absolute value or with a trailing "%" sign. A fractional number
// without a trailing "%" must be recognized by the presence of a
// decimal separator; numbers without decimal separators are plain
// sizes in bytes (separate case in the parsing).
// The first part of the regexp matches NNN.[MMM]; the second part
// [NNN].MMM, and the last part matches explicit percentages with or
// without a decimal separator.
// Values smaller than 1% and 100% are rejected after parsing using
// a separate check.
var fractionRegex = regexp.MustCompile(`^([0-9]+\.[0-9]*|[0-9]*\.[0-9]+|[0-9]+(\.[0-9]*)?%)$`)

// NewStoreSpec parses the string passed into a --store flag and returns a
// StoreSpec if it is correctly parsed.
// There are four possible fields that can be passed in, comma separated:
// - path=xxx The directory in which to the rocks db instance should be
//   located, required unless using a in memory storage.
// - type=mem This specifies that the store is an in memory storage instead of
//   an on disk one. mem is currently the only other type available.
// - size=xxx The optional maximum size of the storage. This can be in one of a
//   few different formats.
//   - 10000000000     -> 10000000000 bytes
//   - 20GB            -> 20000000000 bytes
//   - 20GiB           -> 21474836480 bytes
//   - 0.02TiB         -> 21474836480 bytes
//   - 20%             -> 20% of the available space
//   - 0.2             -> 20% of the available space
// - attrs=xxx:yyy:zzz A colon separated list of optional attributes.
// Note that commas are forbidden within any field name or value.
func NewStoreSpec(value string) (StoreSpec, error) {
	if len(value) == 0 {
		return StoreSpec{}, fmt.Errorf("no value specified")
	}
	var ss StoreSpec
	used := make(map[string]struct{})
	for _, split := range strings.Split(value, ",") {
		if len(split) == 0 {
			continue
		}
		subSplits := strings.SplitN(split, "=", 2)
		var field string
		var value string
		if len(subSplits) == 1 {
			field = "path"
			value = subSplits[0]
		} else {
			field = strings.ToLower(subSplits[0])
			value = subSplits[1]
		}
		if _, ok := used[field]; ok {
			return StoreSpec{}, fmt.Errorf("%s field was used twice in store definition", field)
		}
		used[field] = struct{}{}

		if len(field) == 0 {
			continue
		}
		if len(value) == 0 {
			return StoreSpec{}, fmt.Errorf("no value specified for %s", field)
		}

		switch field {
		case "path":
			if value[0] == '~' {
				return StoreSpec{}, fmt.Errorf("store path cannot start with '~': %s", value)
			}
			// Ensure that the store paths are absolute. This will clarify the
			// output of the startup messages and ensure that logging doesn't
			// get confused if the current working directory were to change for
			// any reason.
			var err error
			ss.Path, err = filepath.Abs(value)
			if err != nil {
				return StoreSpec{}, errors.Wrapf(err, "could not find absolute path for %s", value)
			}
		case "size":
			if fractionRegex.MatchString(value) {
				percentFactor := 100.0
				factorValue := value
				if value[len(value)-1] == '%' {
					percentFactor = 1.0
					factorValue = value[:len(value)-1]
				}
				var err error
				ss.SizePercent, err = strconv.ParseFloat(factorValue, 64)
				ss.SizePercent *= percentFactor
				if err != nil {
					return StoreSpec{}, fmt.Errorf("could not parse store size (%s) %s", value, err)
				}
				if ss.SizePercent > 100 || ss.SizePercent < 1 {
					return StoreSpec{}, fmt.Errorf("store size (%s) must be between 1%% and 100%%", value)
				}
			} else {
				var err error
				ss.SizeInBytes, err = humanizeutil.ParseBytes(value)
				if err != nil {
					return StoreSpec{}, fmt.Errorf("could not parse store size (%s) %s", value, err)
				}
				if ss.SizeInBytes < MinimumStoreSize {
					return StoreSpec{}, fmt.Errorf("store size (%s) must be larger than %s", value,
						humanizeutil.IBytes(MinimumStoreSize))
				}
			}
		case "attrs":
			// Check to make sure there are no duplicate attributes.
			attrMap := make(map[string]struct{})
			for _, attribute := range strings.Split(value, ":") {
				if _, ok := attrMap[attribute]; ok {
					return StoreSpec{}, fmt.Errorf("duplicate attribute given for store: %s", attribute)
				}
				attrMap[attribute] = struct{}{}
			}
			for attribute := range attrMap {
				ss.Attributes.Attrs = append(ss.Attributes.Attrs, attribute)
			}
			sort.Strings(ss.Attributes.Attrs)
		case "type":
			if value == "mem" {
				ss.InMemory = true
			} else {
				return StoreSpec{}, fmt.Errorf("%s is not a valid store type", value)
			}
		default:
			return StoreSpec{}, fmt.Errorf("%s is not a valid store field", field)
		}
	}
	if ss.InMemory {
		// Only in memory stores don't need a path and require a size.
		if ss.Path != "" {
			return StoreSpec{}, fmt.Errorf("path specified for in memory store")
		}
		if ss.SizePercent == 0 && ss.SizeInBytes == 0 {
			return StoreSpec{}, fmt.Errorf("size must be specified for an in memory store")
		}
	} else if ss.Path == "" {
		return StoreSpec{}, fmt.Errorf("no path specified")
	}
	return ss, nil
}

// StoreSpecList contains a slice of StoreSpecs that implements pflag's value
// interface.
type StoreSpecList struct {
	Specs   []StoreSpec
	updated bool // updated is used to determine if specs only contain the default value.
}

var _ pflag.Value = &StoreSpecList{}

// String returns a string representation of all the StoreSpecs. This is part
// of pflag's value interface.
func (ssl StoreSpecList) String() string {
	var buffer bytes.Buffer
	for _, ss := range ssl.Specs {
		fmt.Fprintf(&buffer, "--store=%s ", ss)
	}
	// Trim the extra space from the end if it exists.
	if l := buffer.Len(); l > 0 {
		buffer.Truncate(l - 1)
	}
	return buffer.String()
}

// Type returns the underlying type in string form. This is part of pflag's
// value interface.
func (ssl *StoreSpecList) Type() string {
	return "StoreSpec"
}

// Set adds a new value to the StoreSpecValue. It is the important part of
// pflag's value interface.
func (ssl *StoreSpecList) Set(value string) error {
	spec, err := NewStoreSpec(value)
	if err != nil {
		return err
	}
	if !ssl.updated {
		ssl.Specs = []StoreSpec{spec}
		ssl.updated = true
	} else {
		ssl.Specs = append(ssl.Specs, spec)
	}
	return nil
}

// JoinListType is a slice of strings that implements pflag's value
// interface.
type JoinListType []string

// String returns a string representation of all the JoinListType. This is part
// of pflag's value interface.
func (jls JoinListType) String() string {
	var buffer bytes.Buffer
	for _, jl := range jls {
		fmt.Fprintf(&buffer, "--join=%s ", jl)
	}
	// Trim the extra space from the end if it exists.
	if l := buffer.Len(); l > 0 {
		buffer.Truncate(l - 1)
	}
	return buffer.String()
}

// Type returns the underlying type in string form. This is part of pflag's
// value interface.
func (jls *JoinListType) Type() string {
	return "string"
}

// Set adds a new value to the JoinListType. It is the important part of
// pflag's value interface.
func (jls *JoinListType) Set(value string) error {
	*jls = append(*jls, value)
	return nil
}
