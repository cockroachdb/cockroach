// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/storage/storageconfig"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble"
	"github.com/dustin/go-humanize"
	"github.com/spf13/pflag"
)

// This file implements method receivers for server.Config struct
// -- 'Stores', which satisfies pflag's value interface

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

// StoreSpec contains the details that can be specified in the cli pertaining
// to the --store flag.
type StoreSpec = storageconfig.Store

// StoreSpecCmdLineString returns a fully parsable version of the store spec.
func StoreSpecCmdLineString(ss storageconfig.Store) string {
	// TODO(jackson): Implement redact.SafeFormatter
	var buffer bytes.Buffer
	if len(ss.Path) != 0 {
		fmt.Fprintf(&buffer, "path=%s,", ss.Path)
	}
	switch ss.Type {
	case storageconfig.StoreTypeInMemory:
		fmt.Fprint(&buffer, "type=mem,")
	case storageconfig.StoreTypeBasalt:
		fmt.Fprint(&buffer, "type=basalt,")
	case storageconfig.StoreTypeLocal:
		// default; no type field emitted
	}
	if ss.Size.IsBytes() {
		fmt.Fprintf(&buffer, "size=%s,", humanizeutil.IBytes(ss.Size.Bytes()))
	} else if ss.Size.IsPercent() {
		fmt.Fprintf(&buffer, "size=%s%%,", humanize.Ftoa(ss.Size.Percent()))
	}
	if ss.BallastSize.IsBytes() {
		fmt.Fprintf(&buffer, "ballast-size=%s,", humanizeutil.IBytes(ss.BallastSize.Bytes()))
	} else if ss.BallastSize.IsPercent() {
		fmt.Fprintf(&buffer, "ballast-size=%s%%,", humanize.Ftoa(ss.BallastSize.Percent()))
	}
	if len(ss.Attributes) > 0 {
		fmt.Fprint(&buffer, "attrs=")
		for i, attr := range ss.Attributes {
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
	if ss.ProvisionedRate.ProvisionedBandwidth > 0 {
		fmt.Fprintf(&buffer, "provisioned-rate=bandwidth=%s/s,",
			humanizeutil.IBytes(ss.ProvisionedRate.ProvisionedBandwidth))
	}
	// Trim the extra comma from the end if it exists.
	if l := buffer.Len(); l > 0 {
		buffer.Truncate(l - 1)
	}
	return buffer.String()
}

// storeSpecFields is the set of recognized field names in a --store flag value.
// Used by NewStoreSpec to distinguish field separators from commas embedded in
// values (e.g., basalt URLs with multiple controller addresses).
var storeSpecFields = map[string]struct{}{
	"path":             {},
	"type":             {},
	"size":             {},
	"ballast-size":     {},
	"attrs":            {},
	"pebble":           {},
	"provisioned-rate": {},
}

// NewStoreSpec parses the string passed into a --store flag and returns a
// StoreSpec if it is correctly parsed.
// There are seven possible fields that can be passed in, comma separated:
//   - path=xxx The directory in which the rocks db instance should be
//     located, required unless using an in memory storage.
//   - type=local/mem/basalt This specifies that the store uses a local on-disk,
//     in-memory or basalt storage.
//   - size=xxx The optional maximum size of the storage. This can be in one of a
//     few different formats:
//     -- 10000000000     -> 10000000000 bytes
//     -- 20GB            -> 20000000000 bytes
//     -- 20GiB           -> 21474836480 bytes
//     -- 0.02TiB         -> 21474836480 bytes
//     -- 20%             -> 20% of the available space
//     -- 0.2             -> 20% of the available space
//   - ballast-size=xxx The optional size of the ballast file. Uses the same
//     format as the size field.
//   - attrs=xxx:yyy:zzz A colon separated list of optional attributes.
//   - pebble=xxx The optional string for specifying Pebble options.
//   - provisioned-rate=bandwidth=<bandwidth-bytes/s> The provisioned-rate can be
//     used for admission control for operations on the store and if unspecified,
//     a cluster setting (kvadmission.store.provisioned_bandwidth) will be used.
//
// Note that commas are forbidden within field names and most field values,
// since they are used to separate fields. However, basalt store paths (for
// example: "basalt://addr1,addr2/cluster-id/store-id") may contain commas as
// part of the controller address list. The parser handles this by only
// splitting on commas that are followed by a recognized field name and '='.
func NewStoreSpec(spec string) (StoreSpec, error) {
	const pathField = "path"
	if len(spec) == 0 {
		return StoreSpec{}, fmt.Errorf("no value specified")
	}
	var ss StoreSpec
	used := make(map[string]struct{})
	typeSet := false
	splits := strings.Split(spec, ",")
	for splitIdx := 0; splitIdx < len(splits); splitIdx++ {
		split := splits[splitIdx]
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

		if _, ok := storeSpecFields[field]; !ok {
			return StoreSpec{}, fmt.Errorf("%s is not a valid store field", field)
		}
		if len(value) == 0 {
			return StoreSpec{}, fmt.Errorf("no value specified for %s", field)
		}

		switch field {
		case pathField:
			if strings.HasPrefix(value, storageconfig.BasaltPathPrefix) {
				if typeSet && ss.Type != storageconfig.StoreTypeBasalt {
					return StoreSpec{}, fmt.Errorf("cannot set a basalt path with store type %s", ss.Type)
				}
				ss.Type = storageconfig.StoreTypeBasalt
				typeSet = true

				// We find the all the subsequent fields that do not contain a
				// valid field name followed by the '=' character, which implies
				// that they are a part of the basalt controller URL, and
				// combine them into the current path.
				for _, next := range splits[splitIdx+1:] {
					eqIdx := strings.IndexByte(next, '=')
					if eqIdx != -1 {
						fName := strings.ToLower(next[:eqIdx])
						if _, ok := storeSpecFields[fName]; ok {
							break // next segment is a new field
						}
					}
					// Not a recognized field — this comma was part of the
					// basalt URL. Merge this into the current path and advance
					// the iteration index for the outer loop, marking the next
					// split as "consumed".
					value += "," + next
					splitIdx++
				}
			}
			ss.Path = value
		case "size":
			var err error
			ss.Size, err = storageconfig.ParseSizeSpec(value)
			if err != nil {
				return StoreSpec{}, err
			}
		case "ballast-size":
			ballastSize, err := storageconfig.ParseSizeSpec(value)
			if err != nil {
				return StoreSpec{}, errors.Wrap(err, "ballast")
			}
			ss.BallastSize = ballastSize
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
				ss.Attributes = append(ss.Attributes, attribute)
			}
			sort.Strings(ss.Attributes)
		case "type":
			if typeSet && !strings.EqualFold(value, "basalt") {
				return StoreSpec{}, fmt.Errorf("cannot set store type %s with a basalt path", value)
			}
			// We use strings.EqualFold to maintain parity with the YAML parsing
			// code for StoreType.
			switch {
			case strings.EqualFold(value, "mem"):
				ss.Type = storageconfig.StoreTypeInMemory
			case strings.EqualFold(value, "basalt"):
				ss.Type = storageconfig.StoreTypeBasalt
			case strings.EqualFold(value, "local"):
				ss.Type = storageconfig.StoreTypeLocal
			default:
				return StoreSpec{}, fmt.Errorf("%s is not a valid store type", value)
			}
			typeSet = true
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
			var pebbleOptionsErrs error
			err := opts.Parse(buf.String(), &pebble.ParseHooks{
				OnUnknown: func(name, value string) {
					pebbleOptionsErrs = errors.CombineErrors(pebbleOptionsErrs,
						errors.Newf("unknown option: %s=%s", name, value))
				},
			})
			err = errors.CombineErrors(err, pebbleOptionsErrs)
			if err != nil {
				return StoreSpec{}, err
			}
			ss.PebbleOptions = buf.String()
		case "provisioned-rate":
			rateSpec, err := storageconfig.ParseProvisionedRate(value)
			if err != nil {
				return StoreSpec{}, err
			}
			ss.ProvisionedRate = rateSpec
		}
	}
	if err := ss.Validate(); err != nil {
		return StoreSpec{}, err
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
		fmt.Fprintf(&buffer, "--%s=%s ", cliflags.Store.Name, StoreSpecCmdLineString(ss))
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

// EmergencyBallastFile returns the path (relative to a data directory) used
// for an emergency ballast file. The returned path must be stable across
// releases (eg, we cannot change these constants), otherwise we may duplicate
// ballasts.
func EmergencyBallastFile(pathJoin func(...string) string, dataDir string) string {
	return pathJoin(dataDir, AuxiliaryDir, "EMERGENCY_BALLAST")
}

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
		if !ss.IsLocal() {
			continue
		}
		path := PreventedStartupFile(filepath.Join(ss.Path, AuxiliaryDir))
		if path == "" {
			continue
		}
		b, err := os.ReadFile(path)
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
