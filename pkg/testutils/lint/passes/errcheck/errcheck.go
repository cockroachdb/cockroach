// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package errcheck defines an Analyzer that is a slightly modified version
// of the errcheck Analyzer from upstream (github.com/kisielk/errcheck).
// Specifically, we pass in our own list of excludes.

//go:build bazel
// +build bazel

package errcheck

import (
	_ "embed"

	"github.com/kisielk/errcheck/errcheck"
)

var Analyzer = errcheck.Analyzer

//go:embed errcheck_excludes.txt
var excludesContent string

func init() {
	if err := Analyzer.Flags.Set("excludes", excludesContent); err != nil {
		panic(err)
	}
}
