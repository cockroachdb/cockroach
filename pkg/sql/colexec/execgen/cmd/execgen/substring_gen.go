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

	s = strings.ReplaceAll(s, "_START_WIDTH", "{{$startWidth}}{{if eq $startWidth -1}}: default{{end}}")
	s = strings.ReplaceAll(s, "_LENGTH_WIDTH", "{{$lengthWidth}}{{if eq $lengthWidth -1}}: default{{end}}")
	s = strings.ReplaceAll(s, "_StartType", "Int{{if eq $startWidth -1}}64{{else}}{{$startWidth}}{{end}}")
	s = strings.ReplaceAll(s, "_LengthType", "Int{{if eq $lengthWidth -1}}64{{else}}{{$lengthWidth}}{{end}}")

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
