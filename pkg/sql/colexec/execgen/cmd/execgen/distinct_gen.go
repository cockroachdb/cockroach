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
	"fmt"
	"io"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
)

// distinctTmpl is the common base for the template used to generate code for
// ordered distinct structs and sort partitioners. It should be used as a format
// string with two %s arguments:
// 1. specifies the package that the generated code is placed in
// 2. specifies which of the building blocks coming from distinct_tmpl.go should
// be included into the code generation.
const distinctTmpl = `
// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package %s

import (
	"context"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

%s
`

const distinctOpsTmpl = "pkg/sql/colexec/colexecbase/distinct_tmpl.go"

func genDistinctOps(targetPkg, targetTmpl string) generator {
	return func(inputFileContents string, wr io.Writer) error {
		r := strings.NewReplacer(
			"_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}",
			"_TYPE_WIDTH", typeWidthReplacement,
			"_GOTYPESLICE", "{{.GoTypeSliceName}}",
			"_GOTYPE", "{{.GoType}}",
			"_TYPE", "{{.VecMethod}}",
			"TemplateType", "{{.VecMethod}}")
		s := r.Replace(inputFileContents)

		assignNeRe := makeFunctionRegex("_ASSIGN_NE", 6)
		s = assignNeRe.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 6))

		s = replaceManipulationFuncs(s)

		// Now, generate the op, from the template.
		tmpl, err := template.New("distinct_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
		if err != nil {
			return err
		}

		tmpl, err = tmpl.Parse(fmt.Sprintf(distinctTmpl, targetPkg, targetTmpl))
		if err != nil {
			return err
		}
		return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[treecmp.NE])
	}
}

func init() {
	distinctOp := `
{{template "distinctOpConstructor" .}}

{{range .}}
{{range .WidthOverloads}}
{{template "distinctOp" .}}
{{end}}
{{end}}
`
	sortPartitioner := `
{{template "sortPartitionerConstructor" .}}

{{range .}}
{{range .WidthOverloads}}
{{template "sortPartitioner" .}}
{{end}}
{{end}}
`
	registerGenerator(genDistinctOps("colexecbase", distinctOp), "distinct.eg.go", distinctOpsTmpl)
	registerGenerator(genDistinctOps("colexec", sortPartitioner), "sort_partitioner.eg.go", distinctOpsTmpl)
}
