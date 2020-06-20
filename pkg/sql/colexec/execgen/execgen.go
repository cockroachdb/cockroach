// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execgen

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
)

// Generate transforms the string contents of an input execgen template by
// processing all supported // execgen annotations.
func Generate(inputFileContents string, verbose bool) (string, error) {
	f, err := decorator.ParseFile(token.NewFileSet(), "", inputFileContents, parser.ParseComments)
	if err != nil {
		return "", err
	}
	if verbose {
		printFile(f, "parsing")
	}

	// Generate template variants: // execgen:template
	expandTemplates(f, verbose)

	// Inline functions: // execgen:inline
	inlineFuncs(f)

	if verbose {
		printFile(f, "inlining")
	}

	// Produce output string.
	var sb strings.Builder
	_ = decorator.Fprint(&sb, f)
	return sb.String(), nil
}

func printFile(f *dst.File, phase string) {
	var sb strings.Builder
	_ = decorator.Fprint(&sb, f)
	fmt.Fprintf(os.Stderr, "generated code after %s\n", phase)
	fmt.Fprintln(os.Stderr, "-----------------------------------")
	fmt.Fprintf(os.Stderr, "%+v\n", f)
	fmt.Fprintln(os.Stderr, "-----------------------------------")
	fmt.Fprintln(os.Stderr, sb.String())
	fmt.Fprintln(os.Stderr, "-----------------------------------")
}
