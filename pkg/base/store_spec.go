// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble"
	humanize "github.com/dustin/go-humanize"
	"github.com/spf13/pflag"
)

// This file implements method receivers for members of server.Config struct
// -- 'Stores' and 'JoinList', which satisfies pflag's value interface

// MinimumStoreSize is the smallest size in bytes that a store can have. This
// number is based on config's defaultZoneConfig's RangeMaxBytes, which is
// extremely stable. To avoid adding the dependency on config here, it is just
// hard coded to 640MiB.
const MinimumStoreSize = 10 * 64 << 20

// GetAbsoluteStorePath takes a (possibly relative) and returns the absolute path.
// Returns an error if the path begins with '~' or Abs fails.
// 'fieldName' is used in error strings.
func GetAbsoluteStorePath(fieldName string, p string) (string, error) {
	if p[0] == '~' {
		return "", fmt.Errorf("%s cannot start with '~': %s", fieldName, p)
	}

	ret, err := filepath.Abs(p)
	if err != nil {
		return "", errors.Wrapf(err, "could not find absolute path for %s %s", fieldName, p)
	}
	return ret, nil
}

// SizeSpec contains size in different kinds of formats supported by CLI(%age, bytes).
type SizeSpec struct {
	// InBytes is used for calculating free space and making rebalancing
	// decisions. Zero indicates that there is no maximum size. This value is not
	// actually used by the engine and thus not enforced.
	InBytes int64
	Percent float64
}

type intInterval struct {
	min *int64
	max *int64
}

type floatInterval struct {
	min *float64
	max *float64
}

// NewSizeSpec parses the string passed into a --size flag and returns a
// SizeSpec if it is correctly parsed.
func NewSizeSpec(
	value string, bytesRange *intInterval, percentRange *floatInterval,
) (SizeSpec, error) {
	var size SizeSpec
	if fractionRegex.MatchString(value) {
		percentFactor := 100.0
		factorValue := value
		if value[len(value)-1] == '%' {
			percentFactor = 1.0
			factorValue = value[:len(value)-1]
		}
		var err error
		size.Percent, err = strconv.ParseFloat(factorValue, 64)
		size.Percent *= percentFactor
		if err != nil {
			return SizeSpec{}, fmt.Errorf("could not parse store size (%s) %s", value, err)
		}
		if percentRange != nil {
			if (percentRange.min != nil && size.Percent < *percentRange.min) ||
				(percentRange.max != nil && size.Percent > *percentRange.max) {
				return SizeSpec{}, fmt.Errorf(
					"store size (%s) must be between %f%% and %f%%",
					value,
					*percentRange.min,
					*percentRange.max,
				)
			}
		}
	} else {
		var err error
		size.InBytes, err = humanizeutil.ParseBytes(value)
		if err != nil {
			return SizeSpec{}, fmt.Errorf("could not parse store size (%s) %s", value, err)
		}
		if bytesRange != nil {
			if bytesRange.min != nil && size.InBytes < *bytesRange.min {
				return SizeSpec{}, fmt.Errorf("store size (%s) must be larger than %s", value,
					humanizeutil.IBytes(*bytesRange.min))
			}
			if bytesRange.max != nil && size.InBytes > *bytesRange.max {
				return SizeSpec{}, fmt.Errorf("store size (%s) must be smaller than %s", value,
					humanizeutil.IBytes(*bytesRange.max))
			}
		}
	}
	return size, nil
}

// String returns a string representation of the SizeSpec. This is part
// of pflag's value interface.
func (ss *SizeSpec) String() string {
	var buffer bytes.Buffer
	if ss.InBytes != 0 {
		fmt.Fprintf(&buffer, "--size=%s,", humanizeutil.IBytes(ss.InBytes))
	}
	if ss.Percent != 0 {
		fmt.Fprintf(&buffer, "--size=%s%%,", humanize.Ftoa(ss.Percent))
	}
	return buffer.String()
}

// Type returns the underlying type in string form. This is part of pflag's
// value interface.
func (ss *SizeSpec) Type() string {
	return "SizeSpec"
}

var _ pflag.Value = &SizeSpec{}

// Set adds a new value to the StoreSpecValue. It is the important part of
// pflag's value interface.
func (ss *SizeSpec) Set(value string) error {
	spec, err := NewSizeSpec(value, nil, nil)
	if err != nil {
		return err
	}
	ss.InBytes = spec.InBytes
	ss.Percent = spec.Percent
	return nil
}

// StoreSpec contains the details that can be specified in the cli pertaining
// to the --store flag.
type StoreSpec struct {
	Path       string
	Size       SizeSpec
	InMemory   bool
	Attributes roachpb.Attributes
	// StickyInMemoryEngineID is a unique identifier associated with a given
	// store which will remain in memory even after the default Engine close
	// until it has been explicitly cleaned up by CleanupStickyInMemEngine[s]
	// or the process has been terminated.
	// This only applies to in-memory storage engine.
	StickyInMemoryEngineID string
	// UseFileRegistry is true if the "file registry" store version is desired.
	// This is set by CCL code when encryption-at-rest is in use.
	UseFileRegistry bool
	// RocksDBOptions contains RocksDB specific options using a semicolon
	// separated key-value syntax ("key1=value1; key2=value2").
	RocksDBOptions string
	// PebbleOptions contains Pebble-specific options in the same format as a
	// Pebble OPTIONS file but treating any whitespace as a newline:
	// (Eg, "[Options] delete_range_flush_delay=2s flush_split_bytes=4096")
	PebbleOptions string
	// EncryptionOptions is a serialized protobuf set by Go CCL code and passed
	// through to C CCL code to set up encryption-at-rest.  Must be set if and
	// only if encryption is enabled, otherwise left empty.
	EncryptionOptions []byte
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
	if ss.Size.InBytes > 0 {
		fmt.Fprintf(&buffer, "size=%s,", humanizeutil.IBytes(ss.Size.InBytes))
	}
	if ss.Size.Percent > 0 {
		fmt.Fprintf(&buffer, "size=%s%%,", humanize.Ftoa(ss.Size.Percent))
	}
	if len(ss.Attributes.Attrs) > 0 {
		fmt.Fprint(&buffer, "attrs=")
		for i, attr := range ss.Attributes.Attrs {
			if i != 0 {
				fmt.Fprint(&buffer, ":")
			}
			buffer.WriteString(attr)
		}
		fmt.Fprintf(&buffer, ",")
	}
	if len(ss.PebbleOptions) > 0 {
		optsStr := strings.Replace(ss.PebbleOptions, "\n", " ", -1)
		fmt.Fprint(&buffer, "pebble=")
		fmt.Fprint(&buffer, optsStr)
		fmt.Fprint(&buffer, ",")
	}
	// Trim the extra comma from the end if it exists.
	if l := buffer.Len(); l > 0 {
		buffer.Truncate(l - 1)
	}
	return buffer.String()
}

// IsEncrypted returns whether the StoreSpec has encryption enabled.
func (ss StoreSpec) IsEncrypted() bool {
	return len(ss.EncryptionOptions) > 0
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
var fractionRegex = regexp.MustCompile(`^([-]?([0-9]+\.[0-9]*|[0-9]*\.[0-9]+|[0-9]+(\.[0-9]*)?%))$`)

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
	const pathField = "path"
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
			field = pathField
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
		case pathField:
			var err error
			ss.Path, err = GetAbsoluteStorePath(pathField, value)
			if err != nil {
				return StoreSpec{}, err
			}
		case "size":
			var err error
			var minBytesAllowed int64 = MinimumStoreSize
			var minPercent float64 = 1
			var maxPercent float64 = 100
			ss.Size, err = NewSizeSpec(
				value,
				&intInterval{min: &minBytesAllowed},
				&floatInterval{min: &minPercent, max: &maxPercent},
			)
			if err != nil {
				return StoreSpec{}, err
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
		case "rocksdb":
			ss.RocksDBOptions = value
		case "pebble":
			// Pebble options are supplied in the Pebble OPTIONS ini-like
			// format, but allowing any whitespace to delimit lines. Convert
			// the options to a newline-delimited format. This isn't a trivial
			// character replacement because whitespace may appear within a
			// stanza, eg ["Level 0"].
			value = strings.TrimSpace(value)
			var buf bytes.Buffer
			for len(value) > 0 {
				i := strings.IndexFunc(value, func(r rune) bool {
					return r == '[' || unicode.IsSpace(r)
				})
				switch {
				case i == -1:
					buf.WriteString(value)
					value = value[len(value):]
				case value[i] == '[':
					// If there's whitespace within [ ], we write it verbatim.
					j := i + strings.IndexRune(value[i:], ']')
					buf.WriteString(value[:j+1])
					value = value[j+1:]
				case unicode.IsSpace(rune(value[i])):
					// NB: This doesn't handle multibyte whitespace.
					buf.WriteString(value[:i])
					buf.WriteRune('\n')
					value = strings.TrimSpace(value[i+1:])
				}
			}

			// Parse the options just to fail early if invalid. We'll parse
			// them again later when constructing the store engine.
			var opts pebble.Options
			if err := opts.Parse(buf.String(), nil); err != nil {
				return StoreSpec{}, err
			}
			ss.PebbleOptions = buf.String()
		default:
			return StoreSpec{}, fmt.Errorf("%s is not a valid store field", field)
		}
	}
	if ss.InMemory {
		// Only in memory stores don't need a path and require a size.
		if ss.Path != "" {
			return StoreSpec{}, fmt.Errorf("path specified for in memory store")
		}
		if ss.Size.Percent == 0 && ss.Size.InBytes == 0 {
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
		fmt.Fprintf(&buffer, "--%s=%s ", cliflags.Store.Name, ss)
	}
	// Trim the extra space from the end if it exists.
	if l := buffer.Len(); l > 0 {
		buffer.Truncate(l - 1)
	}
	return buffer.String()
}

// AuxiliaryDir is the path of the auxiliary dir relative to an engine.Engine's
// root directory. It must not be changed without a proper migration.
const AuxiliaryDir = "auxiliary"

// PreventedStartupFile is the filename (relative to 'dir') used for files that
// can block server startup.
func PreventedStartupFile(dir string) string {
	return filepath.Join(dir, "_CRITICAL_ALERT.txt")
}

// PriorCriticalAlertError attempts to read the
// PreventedStartupFile for each store directory and returns their
// contents as a structured error.
//
// These files typically request operator intervention after a
// corruption event by preventing the affected node(s) from starting
// back up.
func (ssl StoreSpecList) PriorCriticalAlertError() (err error) {
	addError := func(newErr error) {
		if err == nil {
			err = errors.New("startup forbidden by prior critical alert")
		}
		// We use WithDetailf here instead of errors.CombineErrors
		// because we want the details to be printed to the screen
		// (combined errors only show up via %+v).
		err = errors.WithDetailf(err, "%v", newErr)
	}
	for _, ss := range ssl.Specs {
		path := ss.PreventedStartupFile()
		if path == "" {
			continue
		}
		b, err := ioutil.ReadFile(path)
		if err != nil {
			if !oserror.IsNotExist(err) {
				addError(errors.Wrapf(err, "%s", path))
			}
			continue
		}
		addError(errors.Newf("From %s:\n\n%s\n", path, b))
	}
	return err
}

// PreventedStartupFile returns the path to a file which, if it exists, should
// prevent the server from starting up. Returns an empty string for in-memory
// engines.
func (ss StoreSpec) PreventedStartupFile() string {
	if ss.InMemory {
		return ""
	}
	return PreventedStartupFile(filepath.Join(ss.Path, AuxiliaryDir))
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
	if strings.TrimSpace(value) == "" {
		// No value, likely user error.
		return errors.New("no address specified in --join")
	}
	for _, v := range strings.Split(value, ",") {
		v = strings.TrimSpace(v)
		if v == "" {
			// --join=a,,b  equivalent to --join=a,b
			continue
		}
		// Try splitting the address. This validates the format
		// of the address and tolerates a missing delimiter colon
		// between the address and port number.
		addr, port, err := netutil.SplitHostPort(v, "")
		if err != nil {
			return err
		}
		// Default the port if unspecified.
		if len(port) == 0 {
			port = DefaultPort
		}
		// Re-join the parts. This guarantees an address that
		// will be valid for net.SplitHostPort().
		*jls = append(*jls, net.JoinHostPort(addr, port))
	}
	return nil
}
