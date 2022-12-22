// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import "text/template"

// queryData is data passed to the targets templates.
type queryData struct {
	// All is a bazel query expression representing all of the targets
	// and files in the workspace.
	All string
}

var targets = []struct {
	target        target
	query         string
	doNotGenerate bool
}{
	{
		target: "protobuf",
		query: `
kind(go_proto_library, {{ .All }})`,
	},
	{
		target: "gomock",
		query: `
labels("out", kind("_gomock_prog_exec rule", {{ .All }}))`,
	},
	{
		target: "stringer",
		query: `
labels("outs",  filter("-stringer$", kind("genrule rule", {{ .All }})))`,
	},
	{
		target: "execgen",
		query: `
let genrules = kind("genrule rule",  {{ .All }})
in labels("outs",  attr("tools", "execgen", $genrules)
  + attr("exec_tools", "execgen", $genrules))`,
	},
	{
		target: "optgen",
		query: `
let targets = attr("exec_tools", "(opt|lang)gen",  kind("genrule rule",  {{ .All }}))
in let og = labels("outs",  $targets)
in $og - filter(".*:.*(-gen|gen-).*", $og)`,
	},
	{
		target: "diagrams",
		query:  `labels("outs", //docs/generated/sql/bnf:svg)`,
	},
	{
		target: "bnf",
		query:  `labels("outs", //docs/generated/sql/bnf:bnf)`,
	},
	{
		target: "docs",
		query:  `kind("generated file", //docs/...:*) - ({{ template "diagrams" $ }})`,
	},
	{
		target: "parser",
		query:  `labels("outs", kind("genrule rule", //pkg/sql/sem/... + //pkg/sql/parser/... + //pkg/sql/lexbase/...))`,
	},
	{
		target: "schemachanger",
		query: `
kind("generated file", //pkg/sql/schemachanger/...:*)
  + kind("generated file", //pkg/ccl/schemachangerccl:*)
  - labels("out", kind("_gomock_prog_gen rule", //pkg/sql/schemachanger/...:*))
`,
	},
	{
		target: "excluded",
		query: `
let all = kind("generated file", {{ .All }})
in ($all ^ labels("out", kind("_gomock_prog_gen rule",  {{ .All }})))
  + filter(".*:.*(-gen|gen-).*", $all)
  + //pkg/testutils/lint/passes/errcheck:errcheck_excludes.txt
  + //build/bazelutil:test_force_build_cdeps.txt
  + //build/bazelutil:test_stamping.txt
  + //pkg/cmd/mirror/npm:*
`,
		doNotGenerate: true,
	},
	{
		target: "bindata",
		query: `
kind("bindata", {{ .All }})`,
	},
	{
		target: "misc",
		query: `
kind("generated file", {{ .All }}) - (
    ({{ template "protobuf" $ }})
  + ({{ template "gomock" $ }})
  + ({{ template "stringer" $ }})
  + ({{ template "execgen" $ }})
  + ({{ template "optgen" $ }})
  + ({{ template "docs" $ }})
  + ({{ template "excluded" $ }})
  + ({{ template "parser" $ }})
  + ({{ template "schemachanger" $ }})
  + ({{ template "diagrams" $ }})
  + ({{ template "bnf" $ }})
)`,
	},
}

var queryTemplates = template.New("queries")

func init() {
	for _, q := range targets {
		template.Must(queryTemplates.New(string(q.target)).Parse(q.query))
	}
}
