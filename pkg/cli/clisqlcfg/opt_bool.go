// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlcfg

import "strconv"

// OptBool implements a boolean value that may be undefined.
type OptBool struct {
	isDef bool
	v     bool
}

// Get returns whether the value is defined, and the value.
func (o OptBool) Get() (isDef, val bool) {
	return o.isDef, o.v
}

// String implements the pflag.Value interface.
func (o OptBool) String() string {
	if !o.isDef {
		return "<unspecified>"
	}
	return strconv.FormatBool(o.v)
}

// Type implements the pflag.Value interface.
func (o OptBool) Type() string { return "bool" }

// Set implements the pflag.Value interface.
func (o *OptBool) Set(v string) error {
	b, err := strconv.ParseBool(v)
	if err != nil {
		return err
	}
	o.isDef = true
	o.v = b
	return nil
}
