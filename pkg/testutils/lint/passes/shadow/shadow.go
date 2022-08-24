// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package shadow defines an Analyzer that is a slightly modified version
// of the shadow Analyzer from upstream (golang.org/x/tools).
// We allow shadows of a few variable names, like err.

//go:build bazel
// +build bazel

package shadow

import (
	"fmt"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/shadow"
)

var (
	Analyzer         = shadow.Analyzer
	permittedShadows = []string{
		"ctx",
		"err",
		"pErr",
	}
)

func init() {
	oldRun := Analyzer.Run
	Analyzer.Run = func(p *analysis.Pass) (interface{}, error) {
		pass := *p
		oldReport := p.Report
		pass.Report = func(diag analysis.Diagnostic) {
			for _, permittedShadow := range permittedShadows {
				if strings.HasPrefix(diag.Message, fmt.Sprintf("declaration of %q shadows declaration at line", permittedShadow)) {
					// Can throw the failure away.
					return
				}
			}
			oldReport(diag)
		}
		return oldRun(&pass)
	}
}
