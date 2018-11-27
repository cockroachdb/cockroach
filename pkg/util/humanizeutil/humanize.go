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

package humanizeutil

import (
	"flag"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/dustin/go-humanize"
	"github.com/spf13/pflag"
)

// IBytes is an int64 version of go-humanize's IBytes.
func IBytes(value int64) string {
	if value < 0 {
		return fmt.Sprintf("-%s", humanize.IBytes(uint64(-value)))
	}
	return humanize.IBytes(uint64(value))
}

// ParseBytes is an int64 version of go-humanize's ParseBytes.
func ParseBytes(s string) (int64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("parsing \"\": invalid syntax")
	}
	var startIndex int
	var negative bool
	if s[0] == '-' {
		negative = true
		startIndex = 1
	}
	value, err := humanize.ParseBytes(s[startIndex:])
	if err != nil {
		return 0, err
	}
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("too large: %s", s)
	}
	if negative {
		return -int64(value), nil
	}
	return int64(value), nil
}

// BytesValue is a struct that implements flag.Value and pflag.Value
// suitable to create command-line parameters that accept sizes
// specified using a format recognized by humanize.
// The value is written atomically, so that it is safe to use this
// struct to make a parameter configurable that is used by an
// asynchronous process spawned before command-line argument handling.
// This is useful e.g. for the log file settings which are used
// by the asynchronous log file GC daemon.
type BytesValue struct {
	val   *int64
	isSet bool
}

var _ flag.Value = &BytesValue{}
var _ pflag.Value = &BytesValue{}

// NewBytesValue creates a new pflag.Value bound to the specified
// int64 variable. It also happens to be a flag.Value.
func NewBytesValue(val *int64) *BytesValue {
	return &BytesValue{val: val}
}

// Set implements the flag.Value and pflag.Value interfaces.
func (b *BytesValue) Set(s string) error {
	v, err := ParseBytes(s)
	if err != nil {
		return err
	}
	atomic.StoreInt64(b.val, v)
	b.isSet = true
	return nil
}

// Type implements the pflag.Value interface.
func (b *BytesValue) Type() string {
	return "bytes"
}

// String implements the flag.Value and pflag.Value interfaces.
func (b *BytesValue) String() string {
	// We need to be able to print the zero value in order for go's flags
	// package to not choke when comparing values to the zero value,
	// as it does in isZeroValue as of go1.8:
	// https://github.com/golang/go/blob/release-branch.go1.8/src/flag/flag.go#L384
	if b.val == nil {
		return IBytes(0)
	}
	// This uses the MiB, GiB, etc suffixes. If we use humanize.Bytes() we get
	// the MB, GB, etc suffixes, but the conversion is done in multiples of 1000
	// vs 1024.
	return IBytes(atomic.LoadInt64(b.val))
}

// IsSet returns true iff Set has successfully been called.
func (b *BytesValue) IsSet() bool {
	return b.isSet
}
