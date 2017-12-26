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
	i := int64(defaultValue)
	if opt, ok := o.raw[name]; ok {
		var err error
		i, err = strconv.ParseInt(opt, 0, 64)
		if err != nil && o.err == nil {
			o.err = errors.Wrapf(err, "parsing '%s' option: %s", name, opt)
		}
	}
	if int64(int(x)) != x && o.err == nil {
		o.err = errors.Errorf("parsing int '%s' option overflowed: %d", name, x)
	}
	return int(x)
}

// Err returns the first error encountered during options parsing, if any.
func (o *Opts) Err() error {
	return o.err
}
