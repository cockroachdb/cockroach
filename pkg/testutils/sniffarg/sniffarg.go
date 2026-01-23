// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sniffarg

import (
	"os"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// DoEnv calls Do with os.Args[1:] as the first argument.
func DoEnv(name string, out interface{}) error {
	return Do(os.Args[1:], name, out)
}

// Do looks for the flag `name` (no leading dashes) and sets the corresponding
// `out` values to the value found in `args`.
// Currently, `out` must be of type `*string` or `*bool`, though additional
// types should be straightforward to add as needed.
//
// This is a helper for tests and benchmarks that want to react to flags from
// their environment.
func Do(args []string, name string, out interface{}) error {
	pf := pflag.NewFlagSet("test", pflag.ContinueOnError)
	switch t := out.(type) {
	case *string:
		pf.StringVar(t, name, "", "")
	case *bool:
		pf.BoolVar(t, name, false, "")
	default:
		return errors.Errorf("unsupported type %T", t)
	}
	pf.ParseErrorsWhitelist = pflag.ParseErrorsWhitelist{UnknownFlags: true}
	args = append([]string(nil), args...)
	for i, arg := range args {
		if !strings.HasPrefix(arg, "-") || len(arg) == 1 {
			// NB: `-` does not get transformed into `--` because consider that
			// it's a valid string value, for example `-test.run -`. Unfortunately,
			// `-asd` is also a valid string value. At this level, we really can't
			// tell - we'd need to see the flag definitions. But at least we're not
			// accidentally converting `-` into `--` which would instruct pflag
			// parsing to stop looking for flags after this argument, which could
			// lead to it missing the flag we're looking for.
			// See TestRegressionSingleDashStringValue.
			continue
		}

		// Single-dash flags are not supported by pflag. It will parse them as
		// shorthands. In particular, anything containing `h` (like `-show-logs`)
		// will trigger the "help" flag and causes an error.
		// Transform single-dash args into double-dash args.
		if !strings.HasPrefix(arg, "--") {
			args[i] = "-" + arg
		}
	}
	return pf.Parse(args)
}
