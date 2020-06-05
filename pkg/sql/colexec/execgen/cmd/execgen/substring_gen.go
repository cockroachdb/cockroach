// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"io"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

const substringTmpl = "pkg/sql/colexec/substring_tmpl.go"

func genSubstring(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_START_WIDTH", fmt.Sprintf("{{$startWidth}}{{if eq $startWidth %d}}: default{{end}}", anyWidth),
		"_LENGTH_WIDTH", fmt.Sprintf("{{$lengthWidth}}{{if eq $lengthWidth %d}}: default{{end}}", anyWidth),
		"_StartType", fmt.Sprintf("Int{{if eq $startWidth %d}}64{{else}}{{$startWidth}}{{end}}", anyWidth),
		"_LengthType", fmt.Sprintf("Int{{if eq $lengthWidth %d}}64{{else}}{{$lengthWidth}}{{end}}", anyWidth),
	)
	s := r.Replace(inputFileContents)

	tmpl, err := template.New("substring").Parse(s)
	if err != nil {
		return err
	}

	supportedIntWidths := supportedWidthsByCanonicalTypeFamily[types.IntFamily]
	intWidthsToIntWidths := make(map[int32][]int32)
	for _, intWidth := range supportedIntWidths {
		intWidthsToIntWidths[intWidth] = supportedIntWidths
	}
	return tmpl.Execute(wr, intWidthsToIntWidths)
}

func init() {
	registerGenerator(genSubstring, "substring.eg.go", substringTmpl)
}
