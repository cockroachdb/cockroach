// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"path/filepath"
	"strings"
)

type filePrefixerOption func(*filePrefixerOptions)

var defaultFilePrefixerOptions = filePrefixerOptions{
	Delimiters: []string{"/", ".", "-"},
	Template:   "${host}> ",
}

type filePrefixerOptions struct {
	Delimiters []string
	Template   string
}

func withTemplate(template string) filePrefixerOption {
	return func(o *filePrefixerOptions) {
		o.Template = template
	}
}

type filePrefixer struct {
	template   string
	delimiters []string
}

// newFilePrefixer returns a filePrefixer with default
// values for token delimiters and a template to extract
// path components.
//
// Use FilePrefixerOptions to override default token
// delimiters and template.
func newFilePrefixer(opts ...filePrefixerOption) filePrefixer {
	options := defaultFilePrefixerOptions
	for _, o := range opts {
		o(&options)
	}
	return filePrefixer{
		template:   options.Template,
		delimiters: options.Delimiters,
	}
}

// PopulatePrefixes creates and populates a prefix for each file path directory
// given a collection of log files. The prefixes exclude common paths that don't
// include ",", ".", or "-" delimiters (by default).
//
// File paths are split into tokens using filepath.Separator. If no
// template is provided, template defaults to "${host} > ".
//
// example file paths:
//
//	testdata/merge_logs_v2/nodes/1/cockroach.test-0001.ubuntu.2018-11-30T22_06_47Z.003959.log
//	testdata/merge_logs_v2/nodes/2/cockroach.test-0001.ubuntu.2018-11-30T22_06_47Z.003959.log
//	testdata/merge_logs_v2/nodes/3/cockroach.test-0001.ubuntu.2018-11-30T22_06_47Z.003959.log
//
// prefix provided: (${fpath}) ${host}>
//
// produces:
//
//	(1) test-0001>
//	(2) test-0001>
//	(3) test-0001>
//
// See [debug_merge_logs_test.go, prefixer_test.go] for additional examples.
func (f filePrefixer) PopulatePrefixes(logFiles []fileInfo) {

	tPaths := make([][]string, len(logFiles))
	common := map[string]int{}

	// range over the log files to extract and tokenize the file path dir from
	// the full path. We want to avoid counting any tokens twice
	// so they can be filtered out when generating the prefix later.
	for i, lf := range logFiles {
		seen := map[string]struct{}{}
		tokens := strings.Split(filepath.Dir(lf.path), string(filepath.Separator))
		for _, t := range tokens {
			if _, ok := seen[t]; !ok {
				seen[t] = struct{}{}
				common[t]++
			}
		}
		tPaths[i] = tokens
	}

	// Create prefixes for each file path. The file dir is shortened to exclude
	// common tokens across all file paths. Each token is also checked against
	// a list of delimiters to filter.
	//
	// The shortened dir path is joined to the file name, and is expanded against
	// the regexp pattern.
	//
	for i, tokens := range tPaths {
		var filteredTokens []string

		for _, t := range tokens {
			count := common[t]

			// Include this token if we haven't seen it across all file paths and
			// it doesn't include delimiters.
			if count < len(logFiles) && !f.hasDelimiters(t) {
				filteredTokens = append(filteredTokens, t)
			}
		}
		filteredTokens = append(filteredTokens, filepath.Base(logFiles[i].path))

		shortenedFP := filepath.Join(filteredTokens...)
		matches := logFiles[i].pattern.FindStringSubmatchIndex(shortenedFP)
		logFiles[i].prefix = logFiles[i].pattern.ExpandString(nil, f.template, shortenedFP, matches)
	}
}

func (f filePrefixer) hasDelimiters(token string) bool {
	for _, d := range f.delimiters {
		if strings.Contains(token, d) {
			return true
		}
	}
	return false
}
