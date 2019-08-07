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
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

const overloadsTestUtilsTemplate = `
package exec

import (
  "math"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

{{define "opName"}}perform{{.Name}}{{.LTyp}}{{end}}

{{/* The outer range is a types.T, and the inner is the overloads associated
     with that type. */}}
{{range .}}
{{range .}}

func {{template "opName" .}}(a, b {{.LTyp.GoTypeName}}) {{.RetTyp.GoTypeName}} {
	{{(.Assign "a" "a" "b")}}
	return a
}

{{end}}
{{end}}
`

// genOverloadsTestUtils creates a file that has a function for each overload
// defined in overloads.go. This is so that we can more easily test each
// overload.
func genOverloadsTestUtils(wr io.Writer) error {
	tmpl, err := template.New("overloads_test_utils").Parse(overloadsTestUtilsTemplate)
	if err != nil {
		return err
	}

	typToOverloads := make(map[types.T][]*overload)
	for _, overload := range binaryOpOverloads {
		typ := overload.LTyp
		typToOverloads[typ] = append(typToOverloads[typ], overload)
	}
	return tmpl.Execute(wr, typToOverloads)
}

func init() {
	registerGenerator(genOverloadsTestUtils, "overloads_test_utils.eg.go")
}
