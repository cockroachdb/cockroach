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

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

func genSubstring(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/colexec/substring_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_StartType_T", "coltypes.{{$startType}}", -1)
	s = strings.Replace(s, "_LengthType_T", "coltypes.{{$lengthType}}", -1)
	s = strings.Replace(s, "_StartType", "{{$startType}}", -1)
	s = strings.Replace(s, "_LengthType", "{{$lengthType}}", -1)

	tmpl, err := template.New("substring").Parse(s)
	if err != nil {
		return err
	}

	intToInts := make(map[coltypes.T][]coltypes.T)
	for _, intType := range coltypes.IntTypes {
		intToInts[intType] = coltypes.IntTypes
	}
	return tmpl.Execute(wr, intToInts)
}

func init() {
	registerGenerator(genSubstring, "substring.eg.go")
}
