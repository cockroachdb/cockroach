// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// testfilter is a utility to manipulate JSON streams in [test2json] format.
// Standard input is read and each line starting with `{` and ending with `}`
// parsed (and expected to parse successfully). Lines not matching this pattern
// are classified as output not related to the test and, depending on the args
// passed to `testfilter`, are passed through or removed. The arguments available
// are `--mode=(strip|omit|convert)`, where:
//
// strip: omit output for non-failing tests, pass everything else through. In
//
//	particular, non-test output and tests that never terminate are passed through.
//
// omit: print only failing tests. Note that test2json does not close scopes for
//
//	tests that are running in parallel (in the same package) with a "foreground"
//	test that panics, so it will pass through *only* the one foreground test.
//	Note also that package scopes are omitted; test2json does not reliably close
//	them on panic/Exit anyway.
//
// convert:
//
//	no filtering is performed, but any test2json input is translated back into
//	its pure Go test framework text representation. This is useful for output
//	intended for human eyes.
//
// [test2json]: https://golang.org/cmd/test2json/
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/testfilter"
)

const modeUsage = `strip:
  omit output for non-failing tests, but print run/pass/skip events for all tests
omit:
  only emit failing tests
convert:
  don't perform any filtering, simply convert the json back to original test format'
`

func main() {
	modeFlag := flag.String("mode", "strip", modeUsage)
	flag.Parse()
	if err := testfilter.FilterAndWrite(os.Stdin, os.Stdout, []string{*modeFlag}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
