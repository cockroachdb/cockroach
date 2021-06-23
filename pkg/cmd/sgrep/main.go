// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

func main() {
	cmd := cobra.Command{
		Args: cobra.RangeArgs(0, 1),
	}
	exclude := cmd.Flags().StringP(
		"exclude", "x", "", "Exclude any blocks matching the specified regular expression",
	)
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		var incRE *regexp.Regexp
		if len(args) == 1 {
			var err error
			incRE, err = regexp.Compile(args[0])
			if err != nil {
				return err
			}
		}
		var excRE *regexp.Regexp
		if *exclude != "" {
			var err error
			excRE, err = regexp.Compile(*exclude)
			if err != nil {
				return err
			}
		}
		return runCmd(os.Stdin, os.Stdout, incRE, excRE)
	}
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runCmd(src io.Reader, dst io.Writer, inc, exc *regexp.Regexp) error {
	return run(src, func(block string) {
		if inc != nil && !inc.MatchString(block) {
			return
		}
		if exc != nil && exc.MatchString(block) {
			return
		}
		fmt.Fprintf(dst, "%s\n\n", block)
	})
}

func run(in io.Reader, do func(block string)) error {
	s := bufio.NewScanner(in)
	s.Split(scanBlocks)
	for s.Scan() {
		do(s.Text())
	}
	return s.Err()
}

// scanBlocks is a split function for a Scanner that returns each block of
// consecutive non-blank lines , stripped of any trailing end-of-line marker.
// The returned line may be empty. The end-of-line marker is one optional
// carriage return followed by one mandatory newline. In regular expression
// notation, it is `\r?\n`. The last non-empty line of input will be returned
// even if it has no newline.
func scanBlocks(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := strings.Index(string(data), "\n\n"); i >= 0 {
		return i + 2, dropCR(data[0:i]), nil
	}
	// If we're at EOF, we have a final, non-terminated block . Return it.
	if atEOF {
		return len(data), dropCR(data), nil
	}
	// Request more data.
	return 0, nil, nil
}

// dropCR drops a terminal \r from the data.
func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}
