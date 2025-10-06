// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import "text/template"

// queryData is data passed to the targets templates.
type queryData struct {
	// All is a bazel query expression representing all of the targets
	// and files in the workspace.
	All string
}

var targets = []struct {
	target target
	query  string
	// If doNotGenerate is set, no .bzl file will be generated for this
	// target.
	doNotGenerate bool
	// If this is a non-empty slice, then we don't perform a query at all
	// for this target, and instead use this list as the list of targets to
	// hoist instead.
	hardCodedQueryResults []string
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
  + attr("tools", "execgen", $genrules))`,
	},
	{
		target: "optgen",
		query: `
let targets = attr("tools", "(opt|lang)gen",  kind("genrule rule",  {{ .All }}))
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
  + //pkg/cmd/mirror/npm:*
`,
		doNotGenerate: true,
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
	{
		target: "ui",
		// NB: Note that we don't execute a query to list this genrule.
		// There are very few specifically in `pkg/ui`, and performing this
		// query requires fetching a lot of the JS stuff. We don't need to
		// perform this query just to list one genrule.
		hardCodedQueryResults: []string{
			"//pkg/ui/distccl:genassets",
		},
	},
}

var queryTemplates = template.New("queries")

func init() {
	for _, q := range targets {
		template.Must(queryTemplates.New(string(q.target)).Parse(q.query))
	}
}
