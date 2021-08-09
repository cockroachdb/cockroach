// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// This file contains assorted utilities for working with Bazel internals.

package util

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// OutputOfBinaryRule returns the path of the binary produced by the
// given build target, relative to bazel-bin. That is,
//    filepath.Join(bazelBin, OutputOfBinaryRule(target)) is the absolute
// path to the build binary for the target.
func OutputOfBinaryRule(target string) string {
	colon := strings.Index(target, ":")
	var bin string
	if colon >= 0 {
		bin = target[colon+1:]
	} else {
		bin = target[strings.LastIndex(target, "/")+1:]
	}
	var head string
	if strings.HasPrefix(target, "@") {
		doubleSlash := strings.Index(target, "//")
		head = filepath.Join("external", target[1:doubleSlash])
	} else if colon >= 0 {
		head = strings.TrimPrefix(target[:colon], "//")
	} else {
		head = strings.TrimPrefix(target, "//")
	}
	return filepath.Join(head, bin+"_", bin)
}

// OutputsOfGenrule lists the outputs of a genrule. The first argument
// is the name of the target (e.g. //docs/generated/sql), and the second
// should be the output of `bazel query --output=xml $TARGET`. The
// returned slice is the list of outputs, all of which are relative
// paths atop `bazel-bin` as in `OutputOfBinaryRule`.
func OutputsOfGenrule(target, xmlQueryOutput string) ([]string, error) {
	// XML parsing is a bit heavyweight here, and encoding/xml
	// refuses to parse the query output since it's XML 1.1 instead
	// of 1.0. Have fun with regexes instead.
	colon := strings.LastIndex(target, ":")
	if colon < 0 {
		colon = len(target)
	}
	regexStr := fmt.Sprintf("^<rule-output name=\"%s:(?P<Filename>.*)\"/>$", regexp.QuoteMeta(target[:colon]))
	re, err := regexp.Compile(regexStr)
	if err != nil {
		return nil, err
	}
	var ret []string
	for _, line := range strings.Split(xmlQueryOutput, "\n") {
		line = strings.TrimSpace(line)
		submatch := re.FindStringSubmatch(line)
		if submatch == nil {
			continue
		}
		relBinPath := filepath.Join(strings.TrimPrefix(target[:colon], "//"), submatch[1])
		ret = append(ret, relBinPath)
	}
	return ret, nil
}
