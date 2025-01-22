// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"regexp"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sniffArgs looks for the flags specified in `names` (no leading dashes) and
// sets the corresponding `out` values to the values found in `args`. This only
// works for string flags and in particular does not reliably work for
// "presence" flags, such as bools, since these flags don't carry an explicit
// value in the args.
//
// This is a helper for benchmarks that want to react to flags from their
// environment.
func sniffArgs(inArgs []string, names []string, out []*string) error {
	pf := pflag.NewFlagSet("test", pflag.ContinueOnError)
	for i := 0; i < len(names); i++ {
		pf.StringVar(out[i], names[i], "", "")
	}
	var args []string
	var addNext bool
	for _, arg := range inArgs {
		if addNext {
			addNext = false
			args = append(args, arg)
		}
		for i := 0; i < len(names); i++ {
			if matches := regexp.MustCompile(`^(-{1,2})` + regexp.QuoteMeta(names[i]) + `(=|$)`).FindStringSubmatch(arg); len(
				matches) > 0 {
				if len(matches[1]) == 1 {
					// Transform `-foo` into `--foo` for pflag-style flag.
					arg = "-" + arg
				}
				if len(matches[2]) == 0 {
					// The matched flag is of form `--foo bar` (vs `--foo=bar`), so value
					// is next arg.
					addNext = true
				}
				args = append(args, arg)
				break
			}
		}
	}
	return pf.Parse(args)
}

func TestSniffArgs(t *testing.T) {
	args := []string{
		"-test.benchmem=5",
		"-test.outputdir", "banana",
		"something",
		"--somethingelse", "foo",
		"--boolflag",
	}
	var benchMem string
	var outputDir string
	var somethingElse string
	require.NoError(t, sniffArgs(args, []string{"test.benchmem", "test.outputdir", "somethingelse"},
		[]*string{&benchMem, &outputDir, &somethingElse}))
	assert.Equal(t, "5", benchMem)
	assert.Equal(t, "banana", outputDir)
	assert.Equal(t, "foo", somethingElse)
}
