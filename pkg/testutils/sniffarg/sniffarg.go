// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sniffarg

import (
	"os"
	"regexp"

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
		re := regexp.MustCompile(`^(-{1,2})` + regexp.QuoteMeta(name) + `(=|$)`)
		if matches := re.FindStringSubmatch(arg); len(matches) > 0 && len(matches[1]) == 1 {
			// Transform `-foo` into `--foo` for pflag-style flag.
			args[i] = "-" + arg
		}
	}
	return pf.Parse(args)
}
