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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const ordSyncTmpl = "pkg/sql/colexec/ordered_synchronizer_tmpl.go"

func genOrderedSynchronizer(wr io.Writer) error {
	d, err := ioutil.ReadFile(ordSyncTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.ReplaceAll(s, "_CANONICAL_TYPE_FAMILY", "{{.CanonicalTypeFamilyStr}}")
	s = strings.ReplaceAll(s, "_TYPE_WIDTH", typeWidthReplacement)
	s = strings.ReplaceAll(s, "_GOTYPESLICE", "{{.GoTypeSliceName}}")
	s = strings.ReplaceAll(s, "_TYPE", "{{.VecMethod}}")

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("ordered_synchronizer").Parse(s)
	if err != nil {
		return err
	}

	// It doesn't matter that we're passing in all overloads of Equality
	// comparison operator - we simply need to iterate over all supported
	// types.
	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func init() {
	registerGenerator(genOrderedSynchronizer, "ordered_synchronizer.eg.go", ordSyncTmpl)
}
