// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

type typeWrapper struct {
	types.T
}

// wrappedTypes is a utility slice that wraps types.AllTypes so that a
// consistent receiver name can be used (i.e. "T").
// TODO(asubiotto): I need to make this wrapper so that there is one receiver
// name for the type throughout the file, otherwise multiple replacements need
// to be done since the type is referenced by ".Global" in _FIND_ANY_NOT_NULL
// but just "." elsewhere. Is there any way to avoid this?
var wrappedTypes []typeWrapper

func init() {
	wrappedTypes = make([]typeWrapper, len(types.AllTypes))
	for i := range wrappedTypes {
		wrappedTypes[i].T = types.AllTypes[i]
	}
}

func genAnyNotNullAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/exec/any_not_null_agg_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_GOTYPESLICE", "{{.GoTypeSliceName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "types.{{.}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.}}", -1)

	findAnyNotNull := makeFunctionRegex("_FIND_ANY_NOT_NULL", 4)
	s = findAnyNotNull.ReplaceAllString(s, `{{template "findAnyNotNull" buildDict "T" . "HasNulls" $4}}`)

	s = replaceManipulationFuncs(".T", s)

	tmpl, err := template.New("any_not_null_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, wrappedTypes)
}

func init() {
	registerGenerator(genAnyNotNullAgg, "any_not_null_agg.eg.go")
}
