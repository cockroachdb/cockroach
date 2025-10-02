// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	gohex "encoding/hex"
	"fmt"
	"math"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storageconfig"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/keysutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
	humanize "github.com/dustin/go-humanize"
	"github.com/spf13/pflag"
)

type localityList []roachpb.LocalityAddress

var _ pflag.Value = &localityList{}

// Type implements the pflag.Value interface.
func (l *localityList) Type() string { return "localityList" }

// String implements the pflag.Value interface.
func (l *localityList) String() string {
	string := ""
	for _, loc := range []roachpb.LocalityAddress(*l) {
		string += loc.LocalityTier.Key + "=" + loc.LocalityTier.Value + "@" + loc.Address.String() + ","
	}

	return string
}

// Set implements the pflag.Value interface.
func (l *localityList) Set(value string) error {
	*l = []roachpb.LocalityAddress{}

	values := strings.Split(value, ",")

	for _, value := range values {
		split := strings.Split(value, "@")
		if len(split) != 2 {
			return fmt.Errorf("invalid value for --locality-advertise-address: %s", l)
		}

		tierSplit := strings.Split(split[0], "=")
		if len(tierSplit) != 2 {
			return fmt.Errorf("invalid value for --locality-advertise-address: %s", l)
		}

		tier := roachpb.Tier{}
		tier.Key = tierSplit[0]
		tier.Value = tierSplit[1]

		locAddress := roachpb.LocalityAddress{}
		locAddress.LocalityTier = tier
		locAddress.Address = util.MakeUnresolvedAddr("tcp", split[1])

		*l = append(*l, locAddress)
	}

	return nil
}

// This file contains definitions for data types suitable for use by
// the flag+pflag packages.

type dumpMode int

const (
	dumpBoth dumpMode = iota
	dumpSchemaOnly
	dumpDataOnly
)

// Type implements the pflag.Value interface.
func (m *dumpMode) Type() string { return "string" }

// String implements the pflag.Value interface.
func (m *dumpMode) String() string {
	switch *m {
	case dumpBoth:
		return "both"
	case dumpSchemaOnly:
		return "schema"
	case dumpDataOnly:
		return "data"
	}
	return ""
}

// Set implements the pflag.Value interface.
func (m *dumpMode) Set(s string) error {
	switch s {
	case "both":
		*m = dumpBoth
	case "schema":
		*m = dumpSchemaOnly
	case "data":
		*m = dumpDataOnly
	default:
		return fmt.Errorf("invalid value for --dump-mode: %s", s)
	}
	return nil
}

type mvccKey storage.MVCCKey

// Type implements the pflag.Value interface.
func (k *mvccKey) Type() string { return "engine.MVCCKey" }

// String implements the pflag.Value interface.
func (k *mvccKey) String() string {
	return storage.MVCCKey(*k).String()
}

// Set implements the pflag.Value interface.
func (k *mvccKey) Set(value string) error {
	var typ keyType
	var keyStr string
	i := strings.IndexByte(value, ':')
	if i == -1 {
		keyStr = value
	} else {
		var err error
		typ, err = parseKeyType(value[:i])
		if err != nil {
			return err
		}
		keyStr = value[i+1:]
	}

	switch typ {
	case hex:
		b, err := gohex.DecodeString(keyStr)
		if err != nil {
			return err
		}
		newK, err := storage.DecodeMVCCKey(b)
		if err != nil {
			encoded := gohex.EncodeToString(storage.EncodeMVCCKey(storage.MakeMVCCMetadataKey(roachpb.Key(b))))
			return errors.Wrapf(err, "perhaps this is just a hex-encoded key; you need an "+
				"encoded MVCCKey (i.e. with a timestamp component); here's one with a zero timestamp: %s",
				encoded)
		}
		*k = mvccKey(newK)
	case raw:
		unquoted, err := unquoteArg(keyStr)
		if err != nil {
			return err
		}
		*k = mvccKey(storage.MakeMVCCMetadataKey(roachpb.Key(unquoted)))
	case human:
		scanner := keysutil.MakePrettyScanner(nil /* tableParser */, nil /* tenantParser */)
		key, err := scanner.Scan(keyStr)
		if err != nil {
			return err
		}
		*k = mvccKey(storage.MakeMVCCMetadataKey(key))
	case rangeID:
		fromID, err := parseRangeID(keyStr)
		if err != nil {
			return err
		}
		*k = mvccKey(storage.MakeMVCCMetadataKey(keys.MakeRangeIDPrefix(fromID)))
	default:
		return fmt.Errorf("unknown key type %s", typ)
	}

	return nil
}

// unquoteArg unquotes the provided argument using Go double-quoted
// string literal rules.
func unquoteArg(arg string) (string, error) {
	s, err := strconv.Unquote(`"` + arg + `"`)
	if err != nil {
		return "", errors.Wrapf(err, "invalid argument %q", arg)
	}
	return s, nil
}

type keyType int

//go:generate stringer -type=keyType
const (
	raw keyType = iota
	human
	rangeID
	hex
)

func parseKeyType(value string) (keyType, error) {
	for typ, i := range _keyTypes {
		if strings.EqualFold(value, typ) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("unknown key type '%s'", value)
}

type nodeDecommissionWaitType int

const (
	nodeDecommissionWaitAll nodeDecommissionWaitType = iota
	nodeDecommissionWaitNone
)

// Type implements the pflag.Value interface.
func (s *nodeDecommissionWaitType) Type() string { return "string" }

// String implements the pflag.Value interface.
func (s *nodeDecommissionWaitType) String() string {
	switch *s {
	case nodeDecommissionWaitAll:
		return "all"
	case nodeDecommissionWaitNone:
		return "none"
	default:
		panic("unexpected node decommission wait type (possible values: all, none)")
	}
}

// Set implements the pflag.Value interface.
func (s *nodeDecommissionWaitType) Set(value string) error {
	switch value {
	case "all":
		*s = nodeDecommissionWaitAll
	case "none":
		*s = nodeDecommissionWaitNone
	default:
		return fmt.Errorf("invalid node decommission parameter: %s "+
			"(possible values: all, none)", value)
	}
	return nil
}

type nodeDecommissionCheckMode int

const (
	nodeDecommissionChecksSkip nodeDecommissionCheckMode = iota
	nodeDecommissionChecksEnabled
	nodeDecommissionChecksStrict
)

// Type implements the pflag.Value interface.
func (s *nodeDecommissionCheckMode) Type() string { return "string" }

// String implements the pflag.Value interface.
func (s *nodeDecommissionCheckMode) String() string {
	switch *s {
	case nodeDecommissionChecksSkip:
		return "skip"
	case nodeDecommissionChecksEnabled:
		return "enabled"
	case nodeDecommissionChecksStrict:
		return "strict"
	default:
		panic("unexpected node decommission check mode (possible values: enabled, strict, skip)")
	}
}

// Set implements the pflag.Value interface.
func (s *nodeDecommissionCheckMode) Set(value string) error {
	switch value {
	case "skip":
		*s = nodeDecommissionChecksSkip
	case "enabled":
		*s = nodeDecommissionChecksEnabled
	case "strict":
		*s = nodeDecommissionChecksStrict
	default:
		return fmt.Errorf("invalid node decommission parameter: %s "+
			"(possible values: enabled, strict, skip)", value)
	}
	return nil
}

// bytesOrPercentageValue is a flag that accepts an integer value, an integer
// plus a unit (e.g. 32GB or 32GiB) or a percentage (e.g. 32%). In all these
// cases, it transforms the string flag input into an int64 value.
//
// Since it accepts a percentage, instances need to be configured with
// instructions on how to resolve a percentage to a number (i.e. the answer to
// the question "a percentage of what?"). This is done by taking in a
// percentResolverFunc. There are predefined ones: memoryPercentResolver and
// diskPercentResolverFactory.
//
// bytesOrPercentageValue can be used in two ways:
// 1. Upon flag parsing, it can write an int64 value through a pointer specified
// by the caller.
// 2. It can store the flag value as a string and only convert it to an int64 on
// a subsequent Resolve() call. Input validation still happens at flag parsing
// time.
//
// Option 2 is useful when percentages cannot be resolved at flag parsing time.
// For example, we have flags that can be expressed as percentages of the
// capacity of storage device. Which storage device is in question might only be
// known once other flags are parsed (e.g. --max-disk-temp-storage=10% depends
// on --store).
type bytesOrPercentageValue struct {
	bval *humanizeutil.BytesValue

	origVal string

	// percentResolver is used to turn a percent string into a value. See
	// memoryPercentResolver() and diskPercentResolverFactory().
	percentResolver percentResolverFunc
}

var _ redact.SafeFormatter = (*bytesOrPercentageValue)(nil)

type percentResolverFunc func(percent int) (int64, error)

// memoryPercentResolver turns a percent into the respective fraction of the
// system's internal memory.
func memoryPercentResolver(percent int) (int64, error) {
	sizeBytes, _, err := status.GetTotalMemoryWithoutLogging()
	if err != nil {
		return 0, err
	}
	return (sizeBytes * int64(percent)) / 100, nil
}

// diskPercentResolverFactory takes in a path and produces a percentResolverFunc
// bound to the respective storage device.
//
// An error is returned if dir does not exist.
func diskPercentResolverFactory(dir string) (percentResolverFunc, error) {
	du, err := vfs.Default.GetDiskUsage(dir)
	if err != nil {
		return nil, err
	}
	if du.TotalBytes > math.MaxInt64 {
		return nil, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(du.TotalBytes), humanizeutil.IBytes(math.MaxInt64))
	}
	deviceCapacity := int64(du.TotalBytes)

	return func(percent int) (int64, error) {
		return (deviceCapacity * int64(percent)) / 100, nil
	}, nil
}

// makeBytesOrPercentageValue creates a bytesOrPercentageValue.
//
// v and percentResolver can be nil (either they're both specified or they're
// both nil). If they're nil, then Resolve() has to be called later to get the
// passed-in value.
//
// When using this function, be sure to define the flag Value in a
// context struct (in context.go) and place the call to
// makeBytesOrPercentageValue() in one of the context init
// functions. Do not use global-scope variables.
func makeBytesOrPercentageValue(
	v *int64, percentResolver func(percent int) (int64, error),
) bytesOrPercentageValue {
	return bytesOrPercentageValue{
		bval:            humanizeutil.NewBytesValue(v),
		percentResolver: percentResolver,
	}
}

var fractionRE = regexp.MustCompile(`^0?\.[0-9]+$`)

// Set implements the pflags.Flag interface.
func (b *bytesOrPercentageValue) Set(s string) error {
	b.origVal = s
	if strings.HasSuffix(s, "%") || fractionRE.MatchString(s) {
		multiplier := 100.0
		if s[len(s)-1] == '%' {
			// We have a percentage.
			multiplier = 1.0
			s = s[:len(s)-1]
		}
		// The user can express .123 or 0.123. Parse as float.
		frac, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return err
		}
		percent := int(frac * multiplier)
		if percent < 1 || percent > 99 {
			return fmt.Errorf("percentage %d%% out of range 1%% - 99%%", percent)
		}

		if b.percentResolver == nil {
			// percentResolver not set means that this flag is not yet supposed to set
			// any value.
			return nil
		}

		absVal, err := b.percentResolver(percent)
		if err != nil {
			return err
		}
		s = fmt.Sprint(absVal)
	}
	return b.bval.Set(s)
}

// Resolve can be called to get the flag's value (if any). If the flag had been
// previously set, *v will be written.
func (b *bytesOrPercentageValue) Resolve(v *int64, percentResolver percentResolverFunc) error {
	// The flag was not passed on the command line.
	if b.origVal == "" {
		return nil
	}
	b.percentResolver = percentResolver
	b.bval = humanizeutil.NewBytesValue(v)
	return b.Set(b.origVal)
}

// Type implements the pflag.Value interface.
func (b *bytesOrPercentageValue) Type() string {
	return b.bval.Type()
}

// String implements the pflag.Value interface.
func (b *bytesOrPercentageValue) String() string {
	return redact.StringWithoutMarkers(b)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (b *bytesOrPercentageValue) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Print(b.bval)
}

// IsSet returns true iff Set has successfully been called.
func (b *bytesOrPercentageValue) IsSet() bool {
	return b.bval.IsSet()
}

// populateWithEncryptionOpts iterates through the encryptionSpecList and looks
// for matching paths in the StoreSpecList and WAL failover config. Any
// unmatched EncryptionSpec causes an error.
func populateWithEncryptionOpts(
	storeSpecs base.StoreSpecList,
	walFailoverConfig *storageconfig.WALFailover,
	encryptionSpecs encryptionSpecList,
) error {
	for _, es := range encryptionSpecs.Specs {
		var storeMatched bool
		for i := range storeSpecs.Specs {
			if !es.PathMatches(storeSpecs.Specs[i].Path) {
				continue
			}

			// Found a matching path.
			if storeSpecs.Specs[i].EncryptionOptions != nil {
				return fmt.Errorf("store with path %s already has an encryption setting",
					storeSpecs.Specs[i].Path)
			}

			storeSpecs.Specs[i].EncryptionOptions = &es.Options
			storeMatched = true
			break
		}
		pathMatched, err := maybeSetExternalPathEncryption(&walFailoverConfig.Path, es)
		if err != nil {
			return err
		}
		prevPathMatched, err := maybeSetExternalPathEncryption(&walFailoverConfig.PrevPath, es)
		if err != nil {
			return err
		}
		if !storeMatched && !pathMatched && !prevPathMatched {
			return fmt.Errorf("no usage of path %s found for encryption setting: %v", es.Path, es)
		}
	}
	return nil
}

// maybeSetExternalPathEncryption updates an ExternalPath to contain the provided
// encryption options if the path matches. The ExternalPath must not already have
// an encryption setting.
func maybeSetExternalPathEncryption(
	externalPath *storageconfig.ExternalPath, es storeEncryptionSpec,
) (found bool, err error) {
	if !externalPath.IsSet() || !es.PathMatches(externalPath.Path) {
		return false, nil
	}
	// NB: The external paths WALFailoverConfig.Path and
	// WALFailoverConfig.PrevPath are only ever set in single-store
	// configurations. In multi-store with among-stores failover mode, these
	// will be empty (so we won't encounter the same path twice).
	if externalPath.Encryption != nil {
		return false, fmt.Errorf("WAL failover path %s already has an encryption setting",
			externalPath.Path)
	}
	externalPath.Encryption = &es.Options
	return true, nil
}

// storeEncryptionSpec contains the details that can be specified in the cli via
// the --enterprise-encryption flag.
type storeEncryptionSpec struct {
	Options storageconfig.EncryptionOptions
	Path    string
}

// String returns a fully parsable version of the encryption spec.
func (es storeEncryptionSpec) String() string {
	// All fields are set.
	return fmt.Sprintf("path=%s,key=%s,old-key=%s,rotation-period=%s",
		es.Path, es.Options.KeyFiles.CurrentKey, es.Options.KeyFiles.OldKey, es.Options.RotationPeriod,
	)
}

// PathMatches returns true if this storeEncryptionSpec matches the given store path.
func (es storeEncryptionSpec) PathMatches(path string) bool {
	return es.Path == path || es.Path == "*"
}

// Special value of key paths to mean "no encryption". We do not accept empty fields.
const plaintextFieldValue = "plain"

// parseStoreEncryptionSpec parses the string passed in and returns a new
// storeEncryptionSpec if parsing succeeds.
// TODO(mberhault): we should share the parsing code with the StoreSpec.
func parseStoreEncryptionSpec(value string) (storeEncryptionSpec, error) {
	const pathField = "path"
	es := storeEncryptionSpec{
		Path: "",
		Options: storageconfig.EncryptionOptions{
			KeySource:      storageconfig.EncryptionKeyFromFiles,
			KeyFiles:       &storageconfig.EncryptionKeyFiles{},
			RotationPeriod: storageconfig.DefaultRotationPeriod,
		},
	}

	used := make(map[string]struct{})
	for _, split := range strings.Split(value, ",") {
		if len(split) == 0 {
			continue
		}
		subSplits := strings.SplitN(split, "=", 2)
		if len(subSplits) == 1 {
			return storeEncryptionSpec{}, fmt.Errorf("field not in the form <key>=<value>: %s", split)
		}
		field := strings.ToLower(subSplits[0])
		value := subSplits[1]
		if _, ok := used[field]; ok {
			return storeEncryptionSpec{}, fmt.Errorf("%s field was used twice in encryption definition", field)
		}
		used[field] = struct{}{}

		if len(field) == 0 {
			return storeEncryptionSpec{}, fmt.Errorf("empty field")
		}
		if len(value) == 0 {
			return storeEncryptionSpec{}, fmt.Errorf("no value specified for %s", field)
		}

		switch field {
		case pathField:
			if value == "*" {
				es.Path = value
			} else {
				var err error
				es.Path, err = getAbsoluteFSPath(pathField, value)
				if err != nil {
					return storeEncryptionSpec{}, err
				}
			}
		case "key":
			if value == plaintextFieldValue {
				es.Options.KeyFiles.CurrentKey = plaintextFieldValue
			} else {
				var err error
				es.Options.KeyFiles.CurrentKey, err = getAbsoluteFSPath("key", value)
				if err != nil {
					return storeEncryptionSpec{}, err
				}
			}
		case "old-key":
			if value == plaintextFieldValue {
				es.Options.KeyFiles.OldKey = plaintextFieldValue
			} else {
				var err error
				es.Options.KeyFiles.OldKey, err = getAbsoluteFSPath("old-key", value)
				if err != nil {
					return storeEncryptionSpec{}, err
				}
			}
		case "rotation-period":
			dur, err := time.ParseDuration(value)
			if err != nil {
				return storeEncryptionSpec{}, errors.Wrapf(err, "could not parse rotation-duration value: %s", value)
			}
			es.Options.RotationPeriod = dur
		default:
			return storeEncryptionSpec{}, fmt.Errorf("%s is not a valid enterprise-encryption field", field)
		}
	}

	// Check that all fields are set.
	if es.Path == "" {
		return storeEncryptionSpec{}, fmt.Errorf("no path specified")
	}
	if es.Options.KeyFiles.CurrentKey == "" {
		return storeEncryptionSpec{}, fmt.Errorf("no key specified")
	}
	if es.Options.KeyFiles.OldKey == "" {
		return storeEncryptionSpec{}, fmt.Errorf("no old-key specified")
	}

	return es, nil
}

// encryptionSpecList contains a slice of StoreEncryptionSpecs that implements pflag's value
// interface.
type encryptionSpecList struct {
	Specs []storeEncryptionSpec
}

var _ pflag.Value = &encryptionSpecList{}

// String returns a string representation of all the StoreEncryptionSpecs. This is part
// of pflag's value interface.
func (encl encryptionSpecList) String() string {
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
func (encl *encryptionSpecList) Type() string {
	return "EncryptionSpec"
}

// Set adds a new value to the StoreEncryptionSpecValue. It is the important part of
// pflag's value interface.
func (encl *encryptionSpecList) Set(value string) error {
	spec, err := parseStoreEncryptionSpec(value)
	if err != nil {
		return err
	}
	if encl.Specs == nil {
		encl.Specs = []storeEncryptionSpec{spec}
	} else {
		encl.Specs = append(encl.Specs, spec)
	}
	return nil
}

// encryptionOptionsForStore takes a store directory and returns its EncryptionOptions
// if a matching entry if found in the StoreEncryptionSpecList.
func encryptionOptionsForStore(
	dir string, encryptionSpecs encryptionSpecList,
) (*storageconfig.EncryptionOptions, error) {
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

// getAbsoluteFSPath takes a (possibly relative) and returns the absolute path.
// Returns an error if the path begins with '~' or Abs fails.
// 'fieldName' is used in error strings.
func getAbsoluteFSPath(fieldName string, p string) (string, error) {
	if p[0] == '~' {
		return "", fmt.Errorf("%s cannot start with '~': %s", fieldName, p)
	}

	ret, err := filepath.Abs(p)
	if err != nil {
		return "", errors.Wrapf(err, "could not find absolute path for %s %s", fieldName, p)
	}
	return ret, nil
}

// walFailoverWrapper implements pflag.Value for the wal failover flag.
type walFailoverWrapper struct {
	cfg *storageconfig.WALFailover
}

var _ pflag.Value = (*walFailoverWrapper)(nil)

// Type implements the pflag.Value interface.
func (c *walFailoverWrapper) Type() string { return "string" }

// String implements the pflag.Value interface.
func (c *walFailoverWrapper) String() string {
	return c.cfg.String()
}

// Set implements the pflag.Value interface.
func (c *walFailoverWrapper) Set(s string) error {
	cfg, err := storageconfig.ParseWALFailover(s)
	if err != nil {
		return err
	}
	*c.cfg = cfg
	return nil
}
