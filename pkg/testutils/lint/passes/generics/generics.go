// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package generics reports an error if Go 1.18 generics are used.
package generics

import (
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/usesgenerics"
)

// Analyzer is an analysis.Analyzer that reports uses of go 1.18 generics.
var Analyzer = &analysis.Analyzer{
	Name:     "generics",
	Doc:      `report an error if generics are used`,
	Run:      run,
	Requires: []*analysis.Analyzer{usesgenerics.Analyzer},
}

func run(pass *analysis.Pass) (interface{}, error) {
	features := pass.ResultOf[usesgenerics.Analyzer].(*usesgenerics.Result).Direct
	if features != 0 {
		pass.Report(analysis.Diagnostic{
			Message: "generics are disallowed in CRDB source code",
		})
	}
	return nil, nil
}
