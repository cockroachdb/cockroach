// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"fmt"
	"regexp"
	"strings"
)

type filePrefixerOption func(*filePrefixerOptions)

var defaultFilePrefixerOptions = filePrefixerOptions{
	Delimiters: []string{"/", ".", "_"},
	Splitter:   regexp.MustCompile(`[\.\/]+`),
}

type filePrefixerOptions struct {
	Splitter   *regexp.Regexp
	Delimiters []string
}

//lint:ignore U1001,U1000 used for overriding default values
func withSplitter(s *regexp.Regexp) filePrefixerOption {
	return func(o *filePrefixerOptions) {
		o.Splitter = s
	}
}

//lint:ignore U1001,U1000 used for overriding default values
func withDelimiters(d []string) filePrefixerOption {
	return func(o *filePrefixerOptions) {
		o.Delimiters = d
	}
}

type filePrefixer struct {
	splitter   *regexp.Regexp
	delimiters []string
}

// newFilePrefixer returns a filePrefixer with default
// values for token delimiters and file path splitters.
// Use FilePrefixerOptions to override default token
// delimiters and file path splitters.
func newFilePrefixer(opts ...filePrefixerOption) filePrefixer {
	options := defaultFilePrefixerOptions
	for _, o := range opts {
		o(&options)
	}
	return filePrefixer{
		splitter:   options.Splitter,
		delimiters: options.Delimiters,
	}
}

// Prefix generates a prefix for each file path given a collection
// of log files. The prefixes exclude common paths that don't
// include ",", ".", or "_" delimiters (by default).
//
// File paths are split into tokens using "/" or "." (by default).
// Both delimiters and splitters are configurable using prefixer options.
//
// This produces a clear and short prefix to identify where log
// entries originated from. This is intended to be used when processing
// log files for merging logs.
//
// example file paths:
//   testdata/merge_logs_v2/nodes/1/cockroach.test-0001.regionA.ubuntu.2018-11-30T22_06_47Z.003959.log
//   testdata/merge_logs_v2/nodes/2/cockroach.test-0001.regionB.ubuntu.2018-11-30T22_06_47Z.003959.log
//   testdata/merge_logs_v2/nodes/3/cockroach.test-0001.regionC.ubuntu.2018-11-30T22_06_47Z.003959.log
//
// produces:
//   1/regionA >
//   2/regionB >
//   3/regionC >
//
// For test coverage, see debug_merge_logs_test.go.
//
func (f filePrefixer) Prefix(logFiles []fileInfo) {
	tPaths := make([][]string, len(logFiles))
	common := map[string]int{}

	for i, file := range logFiles {
		seen := map[string]struct{}{}

		tokens := f.splitter.Split(file.path, -1)
		for _, t := range tokens {
			if _, ok := seen[t]; !ok {
				seen[t] = struct{}{}
				common[t]++
			}
		}
		tPaths[i] = tokens
	}

	// Create prefixes for each file path.
	for i, tokens := range tPaths {
		var prefix []string
		// The legacy prefix logic used a pattern and ExpandString to generate the prefix.
		// When a short name was not extracted, the default value was ">".
		logFiles[i].prefix = "> "

		for _, t := range tokens {
			count := common[t]

			// Include this token if we haven't seen it across all file paths and
			// it doesn't include delimiters.
			if count < len(logFiles) && !hasDelimiters(t, f.delimiters) {
				prefix = append(prefix, t)
			}
		}
		if len(prefix) > 0 {
			logFiles[i].prefix = fmt.Sprintf("%s > ", strings.Join(prefix, "/"))
		}
	}
}

func hasDelimiters(token string, del []string) bool {
	for _, d := range del {
		if strings.Contains(token, d) {
			return true
		}
	}
	return false
}
