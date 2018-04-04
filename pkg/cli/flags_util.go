// Copyright 2017 The Cockroach Authors.
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

package cli

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	humanize "github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
	"github.com/pkg/errors"
)

// This file contains definitions for data types suitable for use by
// the flag+pflag packages.

// statementsValue is an implementation of pflag.Value that appends any
// argument to a slice.
type statementsValue []string

// Type implements the pflag.Value interface.
func (s *statementsValue) Type() string { return "statementsValue" }

// String implements the pflag.Value interface.
func (s *statementsValue) String() string {
	return strings.Join(*s, ";")
}

// Set implements the pflag.Value interface.
func (s *statementsValue) Set(value string) error {
	*s = append(*s, value)
	return nil
}

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

type mvccKey engine.MVCCKey

// Type implements the pflag.Value interface.
func (k *mvccKey) Type() string { return "engine.MVCCKey" }

// String implements the pflag.Value interface.
func (k *mvccKey) String() string {
	return engine.MVCCKey(*k).String()
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
	case raw:
		unquoted, err := unquoteArg(keyStr)
		if err != nil {
			return err
		}
		*k = mvccKey(engine.MakeMVCCMetadataKey(roachpb.Key(unquoted)))
	case human:
		key, err := keys.UglyPrint(keyStr)
		if err != nil {
			return err
		}
		*k = mvccKey(engine.MakeMVCCMetadataKey(key))
	case rangeID:
		fromID, err := parseRangeID(keyStr)
		if err != nil {
			return err
		}
		*k = mvccKey(engine.MakeMVCCMetadataKey(keys.MakeRangeIDPrefix(fromID)))
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
)

// _keyTypes stores the names of all the possible key types.
var _keyTypes []string

// keyTypes computes and memoizes the names of all the possible key
// types, based on the definitions produces by Go's stringer (see
// keytype_string.go).
func keyTypes() []string {
	if _keyTypes == nil {
		for i := 0; i+1 < len(_keyType_index); i++ {
			_keyTypes = append(_keyTypes, _keyType_name[_keyType_index[i]:_keyType_index[i+1]])
		}
	}
	return _keyTypes
}

func parseKeyType(value string) (keyType, error) {
	for i, typ := range keyTypes() {
		if strings.EqualFold(value, typ) {
			return keyType(i), nil
		}
	}
	return 0, fmt.Errorf("unknown key type '%s'", value)
}

type nodeDecommissionWaitType int

const (
	nodeDecommissionWaitAll nodeDecommissionWaitType = iota
	nodeDecommissionWaitLive
	nodeDecommissionWaitNone
)

// Type implements the pflag.Value interface.
func (s *nodeDecommissionWaitType) Type() string { return "string" }

// String implements the pflag.Value interface.
func (s *nodeDecommissionWaitType) String() string {
	switch *s {
	case nodeDecommissionWaitAll:
		return "all"
	case nodeDecommissionWaitLive:
		return "live"
	case nodeDecommissionWaitNone:
		return "none"
	}
	return ""
}

// Set implements the pflag.Value interface.
func (s *nodeDecommissionWaitType) Set(value string) error {
	switch value {
	case "all":
		*s = nodeDecommissionWaitAll
	case "live":
		*s = nodeDecommissionWaitLive
	case "none":
		*s = nodeDecommissionWaitNone
	default:
		return fmt.Errorf("invalid node decommission parameter: %s "+
			"(possible values: all, live, none)", value)
	}
	return nil
}

type tableDisplayFormat int

const (
	tableDisplayTSV tableDisplayFormat = iota
	tableDisplayCSV
	tableDisplayPretty
	tableDisplayRecords
	tableDisplaySQL
	tableDisplayHTML
	tableDisplayRaw
	tableDisplayLastFormat
)

// Type implements the pflag.Value interface.
func (f *tableDisplayFormat) Type() string { return "string" }

// String implements the pflag.Value interface.
func (f *tableDisplayFormat) String() string {
	switch *f {
	case tableDisplayTSV:
		return "tsv"
	case tableDisplayCSV:
		return "csv"
	case tableDisplayPretty:
		return "pretty"
	case tableDisplayRecords:
		return "records"
	case tableDisplaySQL:
		return "sql"
	case tableDisplayHTML:
		return "html"
	case tableDisplayRaw:
		return "raw"
	}
	return ""
}

// Set implements the pflag.Value interface.
func (f *tableDisplayFormat) Set(s string) error {
	switch s {
	case "tsv":
		*f = tableDisplayTSV
	case "csv":
		*f = tableDisplayCSV
	case "pretty":
		*f = tableDisplayPretty
	case "records":
		*f = tableDisplayRecords
	case "sql":
		*f = tableDisplaySQL
	case "html":
		*f = tableDisplayHTML
	case "raw":
		*f = tableDisplayRaw
	default:
		return fmt.Errorf("invalid table display format: %s "+
			"(possible values: tsv, csv, pretty, records, sql, html, raw)", s)
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
	val  *int64
	bval *humanizeutil.BytesValue

	origVal string

	// percentResolver is used to turn a percent string into a value. See
	// memoryPercentResolver() and diskPercentResolverFactory().
	percentResolver percentResolverFunc
}
type percentResolverFunc func(percent int) (int64, error)

// memoryPercentResolver turns a percent into the respective fraction of the
// system's internal memory.
func memoryPercentResolver(percent int) (int64, error) {
	sizeBytes, err := server.GetTotalMemory(context.TODO())
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
	fileSystemUsage := gosigar.FileSystemUsage{}
	if err := fileSystemUsage.Get(dir); err != nil {
		return nil, err
	}
	if fileSystemUsage.Total > math.MaxInt64 {
		return nil, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(fileSystemUsage.Total), humanizeutil.IBytes(math.MaxInt64))
	}
	deviceCapacity := int64(fileSystemUsage.Total)

	return func(percent int) (int64, error) {
		return (deviceCapacity * int64(percent)) / 100, nil
	}, nil
}

// newBytesOrPercentageValue creates a bytesOrPercentageValue.
//
// v and percentResolver can be nil (either they're both specified or they're
// both nil). If they're nil, then Resolve() has to be called later to get the
// passed-in value.
func newBytesOrPercentageValue(
	v *int64, percentResolver func(percent int) (int64, error),
) *bytesOrPercentageValue {
	if v == nil {
		v = new(int64)
	}
	return &bytesOrPercentageValue{
		val:             v,
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
	b.val = v
	b.bval = humanizeutil.NewBytesValue(v)
	return b.Set(b.origVal)
}

// Type implements the pflag.Value interface.
func (b *bytesOrPercentageValue) Type() string {
	return b.bval.Type()
}

// String implements the pflag.Value interface.
func (b *bytesOrPercentageValue) String() string {
	return b.bval.String()
}

// IsSet returns true iff Set has successfully been called.
func (b *bytesOrPercentageValue) IsSet() bool {
	return b.bval.IsSet()
}
