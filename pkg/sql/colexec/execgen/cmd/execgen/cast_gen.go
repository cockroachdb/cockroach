// Copyright 2019 The Cockroach Authors.
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
)

const castTmpl = "pkg/sql/colexec/cast_tmpl.go"

func genCastOperators(wr io.Writer) error {
	t, err := ioutil.ReadFile(castTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	assignCast := makeFunctionRegex("_ASSIGN_CAST", 2)
	s = assignCast.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 2))
	s = strings.Replace(s, "_ALLTYPES", "{{$typ}}", -1)
	s = strings.Replace(s, "_FROMTYPE", "{{.FromTyp}}", -1)
	s = strings.Replace(s, "_TOTYPE", "{{.ToTyp}}", -1)
	s = strings.Replace(s, "_GOTYPE", "{{.ToGoTyp}}", -1)

	// replace _FROM_TYPE_SLICE's with execgen.SLICE's of the correct type.
	s = strings.Replace(s, "_FROM_TYPE_SLICE", "execgen.SLICE", -1)
	// replace the _FROM_TYPE_UNSAFEGET's with execgen.UNSAFEGET's of the correct type.
	s = strings.Replace(s, "_FROM_TYPE_UNSAFEGET", "execgen.UNSAFEGET", -1)
	s = replaceManipulationFuncs(".FromTyp", s)

	// replace the _TO_TYPE_SET's with execgen.SET's of the correct type
	s = strings.Replace(s, "_TO_TYPE_SET", "execgen.SET", -1)
	s = replaceManipulationFuncs(".ToTyp", s)

	isCastFuncSet := func(ov castOverload) bool {
		return ov.AssignFunc != nil
	}

	tmpl, err := template.New("cast").Funcs(template.FuncMap{"isCastFuncSet": isCastFuncSet}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, castOverloads)
}

func init() {
	registerGenerator(genCastOperators, "cast.eg.go", castTmpl)
}
