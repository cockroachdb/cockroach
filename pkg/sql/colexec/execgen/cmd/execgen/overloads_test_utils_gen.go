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

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

{{define "opName"}}perform{{.Name}}{{.Left.VecMethod}}{{.Right.VecMethod}}{{end}}

{{range .}}
// {{/*
//     TODO(yuzefovich): overloads on datum-backed types require passing in
//     non-empty valid variable names of columns, and we currently don't have
//     those - we only have "elements". For now, it's ok to skip generation of
//     these utility test methods for datum-backed types.
// */}}
{{if and (not (eq .Left.VecMethod "Datum")) (not (eq .Right.VecMethod "Datum"))}}

func {{template "opName" .}}(a {{.Left.GoType}}, b {{.Right.GoType}}) {{.Right.RetGoType}} {
	var r {{.Right.RetGoType}}
	// In order to inline the templated code of overloads, we need to have a
	// "_overloadHelper" local variable of type "overloadHelper".
	var _overloadHelper overloadHelper
	// However, the scratch is not used in all of the functions, so we add this
	// to go around "unused" error.
	_ = _overloadHelper
	{{(.Right.Assign "r" "a" "b" "" "" "")}}
	return r
}

{{end}}
{{end}}
`

// genOverloadsTestUtils creates a file that has a function for each binary and
// comparison overload supported by the vectorized engine. This is so that we
// can more easily test each overload.
func genOverloadsTestUtils(_ string, wr io.Writer) error {
	tmpl, err := template.New("overloads_test_utils").Parse(overloadsTestUtilsTemplate)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, resolvedBinCmpOpsOverloads)
}

func init() {
	registerGenerator(genOverloadsTestUtils, "overloads_test_utils.eg.go", "")
}
