// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package workload

import (
	"strconv"

	"github.com/pkg/errors"
)

// Opts is a helper for parsing structured options from the string to string map
// used to configure workloads. After the various parse methods are called, the
// Err method should be checked for any errors.
type Opts struct {
	raw map[string]string
	err error
}

// NewOpts makes an Opts from the raw map.
func NewOpts(raw map[string]string) *Opts {
	return &Opts{raw: raw}
}

// Int parses an int from the option with the given name, falling back to the
// given default if it's not present.
func (o *Opts) Int(name string, defaultValue int) int {
	x := int64(defaultValue)
	if opt, ok := o.raw[name]; ok {
		var err error
		x, err = strconv.ParseInt(opt, 0, 64)
		if err != nil && o.err == nil {
			o.err = errors.Wrapf(err, "parsing int '%s' option: %s", name, opt)
		}
	}
	return int(x)
}

// Int64 parses an int64 from the option with the given name, falling back to
// the given default if it's not present.
func (o *Opts) Int64(name string, defaultValue int64) int64 {
	x := defaultValue
	if opt, ok := o.raw[name]; ok {
		var err error
		x, err = strconv.ParseInt(opt, 0, 64)
		if err != nil && o.err == nil {
			o.err = errors.Wrapf(err, "parsing int64 '%s' option: %s", name, opt)
		}
	}
	return x
}

// Bool parses a bool from the option with the given name, falling back to the
// given default if it's not present.
func (o *Opts) Bool(name string, defaultValue bool) bool {
	x := defaultValue
	if opt, ok := o.raw[name]; ok {
		var err error
		x, err = strconv.ParseBool(opt)
		if err != nil && o.err == nil {
			o.err = errors.Wrapf(err, "parsing bool '%s' option: %s", name, opt)
		}
	}
	return x
}

// Err returns the first error encountered during options parsing, if any.
func (o *Opts) Err() error {
	return o.err
}
