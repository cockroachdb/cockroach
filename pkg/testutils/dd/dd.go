// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package dd contains utilities enhancing the experience of using the
// datadriven testing package.
//
// TODO(pav-kv): migrate this to the datadriven package.
package dd

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// ParseCommand parses a datadriven test command into the given struct.
//
// The struct fields are parsed from the TestData input. To assist the parsing,
// all arguments must be annotated with Go tags:
//
//   - name [required]: the name of the argument
//   - opt: whether the argument is optional (can only be "true")
//   - min: the minimal number of values for a variadic argument
//
// All basic Go types are supported, including aliases. For variadic (slice)
// arguments, only []string is supported.
//
// Example:
//
//	var cmd struct {
//		RangeID     roachpb.RangeID   `name:"range-id"`
//		ReplicaID   roachpb.ReplicaID `name:"repl-id"`
//		Initialized bool              `name:"init" opt:"true"`
//		Keys        []string          `name:"keys" min:"2"`
//	}
//	dd.ParseCommand(t, d, &cmd)
//	_ = cmd.RangeID
//
// Example of a valid input line: --range-id=123 --repl-id=4 --keys=(a b)
// See also TestParseCommand for an example use.
func ParseCommand(t *testing.T, d *datadriven.TestData, cmd any) {
	val := reflect.ValueOf(cmd)
	typ := val.Elem().Type()

	for i, n := 0, typ.NumField(); i < n; i++ {
		f := typ.Field(i)
		// Skip the fields without an annotation.
		name, ok := f.Tag.Lookup("name")
		if !ok {
			continue
		}

		// Figure out the minimal number of values in a variadic argument.
		minVals := 0
		variadic := f.Type.Kind() == reflect.Slice
		if minStr := f.Tag.Get("min"); variadic && minStr != "" {
			m, err := strconv.Atoi(minStr)
			require.NoError(t, err)
			minVals = m
		}
		// Figure out whether the argument is optional.
		optional := (variadic && minVals == 0) || f.Tag.Get("opt") == "true"
		optional = optional || f.Type.Kind() == reflect.Bool

		// Scan the argument from TestData, and copy it into the corresponding
		// command struct field.
		arg, ok := d.Arg(name)
		require.True(t, ok || optional, "arg %q is not optional", name)
		if !ok {
			continue
		} else if !variadic {
			arg.ExpectNumVals(t, 1)
			arg.Scan(t, 0, val.Elem().Field(i).Addr().Interface())
		} else {
			arg.ExpectNumValsGE(t, minVals)
			// TODO(pav-kv): support types other than []string.
			val.Elem().Field(i).Set(reflect.ValueOf(arg.Vals))
		}
	}
}
