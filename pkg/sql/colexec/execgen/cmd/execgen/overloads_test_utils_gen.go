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
)

const overloadsTestUtilsTemplate = `
package colexec

import (
	"bytes"
	"math"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

{{define "opName"}}perform{{.Name}}{{.LTyp}}{{.RTyp}}{{end}}

{{/* The range is over all overloads */}}
{{range .}}

func {{template "opName" .}}(a {{.LTyp.GoTypeName}}, b {{.RTyp.GoTypeName}}) {{.RetTyp.GoTypeName}} {
	var r {{.RetTyp.GoTypeName}}
	// In order to inline the templated code of overloads, we need to have a
	// "decimalScratch" local variable of type "decimalOverloadScratch".
	var decimalScratch decimalOverloadScratch
	// However, the scratch is not used in all of the functions, so we add this
	// to go around "unused" error.
	_ = decimalScratch
	{{(.Assign "r" "a" "b")}}
	return r
}

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

	allOverloads := make([]*overload, 0)
	allOverloads = append(allOverloads, binaryOpOverloads...)
	allOverloads = append(allOverloads, comparisonOpOverloads...)
	return tmpl.Execute(wr, allOverloads)
}

func init() {
	registerGenerator(genOverloadsTestUtils, "overloads_test_utils.eg.go", "")
}
