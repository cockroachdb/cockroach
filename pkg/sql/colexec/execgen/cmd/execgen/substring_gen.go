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
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

const substringTmpl = "pkg/sql/colexec/substring_tmpl.go"

func genSubstring(wr io.Writer) error {
	t, err := ioutil.ReadFile(substringTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.ReplaceAll(s, "_START_WIDTH", fmt.Sprintf("{{$startWidth}}{{if eq $startWidth %d}}: default{{end}}", anyWidth))
	s = strings.ReplaceAll(s, "_LENGTH_WIDTH", fmt.Sprintf("{{$lengthWidth}}{{if eq $lengthWidth %d}}: default{{end}}", anyWidth))
	s = strings.ReplaceAll(s, "_StartType", fmt.Sprintf("Int{{if eq $startWidth %d}}64{{else}}{{$startWidth}}{{end}}", anyWidth))
	s = strings.ReplaceAll(s, "_LengthType", fmt.Sprintf("Int{{if eq $lengthWidth %d}}64{{else}}{{$lengthWidth}}{{end}}", anyWidth))

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
