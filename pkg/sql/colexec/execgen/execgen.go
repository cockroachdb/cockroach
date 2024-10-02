// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execgen

import (
	"go/parser"
	"go/token"
	"strings"

	"github.com/dave/dst/decorator"
)

// Generate transforms the string contents of an input execgen template by
// processing all supported // execgen annotations.
func Generate(inputFileContents string) (string, error) {
	f, err := decorator.ParseFile(token.NewFileSet(), "", inputFileContents, parser.ParseComments)
	if err != nil {
		return "", err
	}

	// Generate template variants: // execgen:template
	expandTemplates(f)

	// Inline functions: // execgen:inline
	inlineFuncs(f)

	// Produce output string.
	var sb strings.Builder
	if err := decorator.Fprint(&sb, f); err != nil {
		panic(err)
	}
	return sb.String(), nil
}
