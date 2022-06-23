// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"strconv"

	"github.com/mmatczuk/go_generics/globals"
)

type importedPackage struct {
	newName string
	path    string
}

// updateImportIdent modifies the given import identifier with the new name
// stored in the used map. If the identifier doesn't exist in the used map yet,
// a new name is generated and inserted into the map.
func updateImportIdent(orig string, imports mapValue, id *ast.Ident, used map[string]*importedPackage) error {
	importName := id.Name

	// If the name is already in the table, just use the new name.
	m := used[importName]
	if m != nil {
		id.Name = m.newName
		return nil
	}

	// Create a new entry in the used map.
	path := imports[importName]
	if path == "" {
		return fmt.Errorf("Unknown path to package '%s', used in '%s'", importName, orig)
	}

	m = &importedPackage{
		newName: fmt.Sprintf("__generics_imported%d", len(used)),
		path:    strconv.Quote(path),
	}
	used[importName] = m

	id.Name = m.newName

	return nil
}

// convertExpression creates a new string that is a copy of the input one with
// all imports references renamed to the names in the "used" map. If the
// referenced import isn't in "used" yet, a new one is created based on the path
// in "imports" and stored in "used". For example, if string s is
// "math.MaxUint32-math.MaxUint16+10", it would be converted to
// "x.MaxUint32-x.MathUint16+10", where x is a generated name.
func convertExpression(s string, imports mapValue, used map[string]*importedPackage) (string, error) {
	// Parse the expression in the input string.
	expr, err := parser.ParseExpr(s)
	if err != nil {
		return "", fmt.Errorf("Unable to parse \"%s\": %v", s, err)
	}

	// Go through the AST and update references.
	var retErr error
	ast.Inspect(expr, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.SelectorExpr:
			if id := globals.GetIdent(x.X); id != nil {
				if err := updateImportIdent(s, imports, id, used); err != nil {
					retErr = err
				}
				return false
			}
		}
		return true
	})
	if retErr != nil {
		return "", retErr
	}

	// Convert the modified AST back to a string.
	fset := token.NewFileSet()
	var buf bytes.Buffer
	if err := format.Node(&buf, fset, expr); err != nil {
		return "", err
	}

	return string(buf.Bytes()), nil
}

// updateImports replaces all maps in the input slice with copies where the
// mapped values have had all references to imported packages renamed to
// generated names. It also returns an import declaration for all the renamed
// import packages.
//
// For example, if the input maps contains A=math.B and C=math.D, the updated
// maps will instead contain A=__generics_imported0.B and
// C=__generics_imported0.C, and the 'import __generics_imported0 "math"' would
// be returned as the import declaration.
func updateImports(maps []mapValue, imports mapValue) (ast.Decl, error) {
	importsUsed := make(map[string]*importedPackage)

	// Update all maps.
	for i, m := range maps {
		newMap := make(mapValue)
		for n, e := range m {
			updated, err := convertExpression(e, imports, importsUsed)
			if err != nil {
				return nil, err
			}

			newMap[n] = updated
		}
		maps[i] = newMap
	}

	// Nothing else to do if no imports are used in the expressions.
	if len(importsUsed) == 0 {
		return nil, nil
	}

	// Create spec array for each new import.
	specs := make([]ast.Spec, 0, len(importsUsed))
	for _, i := range importsUsed {
		specs = append(specs, &ast.ImportSpec{
			Name: &ast.Ident{Name: i.newName},
			Path: &ast.BasicLit{Value: i.path},
		})
	}

	return &ast.GenDecl{
		Tok:    token.IMPORT,
		Specs:  specs,
		Lparen: token.NoPos + 1,
	}, nil
}
