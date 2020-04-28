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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

{{define "opName"}}perform{{.Name}}{{.Left.VecMethod}}{{.Right.VecMethod}}{{end}}

{{range .}}

func {{template "opName" .}}(a {{.Left.GoType}}, b {{.Right.GoType}}) {{.Right.RetGoType}} {
	var r {{.Right.RetGoType}}
	// In order to inline the templated code of overloads, we need to have a
	// "decimalScratch" local variable of type "decimalOverloadScratch".
	var decimalScratch decimalOverloadScratch
	// However, the scratch is not used in all of the functions, so we add this
	// to go around "unused" error.
	_ = decimalScratch
	{{(.Right.Assign "r" "a" "b")}}
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

	return tmpl.Execute(wr, getProjTmplInfo().ResolvedBinCmpOps)
}

func init() {
	registerGenerator(genOverloadsTestUtils, "overloads_test_utils.eg.go", "")
}
