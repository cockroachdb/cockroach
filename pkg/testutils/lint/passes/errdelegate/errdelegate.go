// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package errdelegate defines an Analyzer that detects Err() methods
// that don't delegate to underlying readers/writers that also have Err().
package errdelegate

import (
	"go/ast"
	"go/types"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = `check for Err() methods that don't delegate to underlying types

This analyzer detects patterns like:

  type myReader struct {
      underlying *goavro.OCFReader
      err        error
  }

  func (m *myReader) Err() error {
      return m.err  // BUG: doesn't check underlying.Err()
  }

When a type wraps another type that has an Err() method, the wrapper's
Err() method should typically delegate to or combine with the underlying
Err(). This pattern was the root cause of AVRO OCF data loss issues.`

const name = "errdelegate"

var errorType = types.Universe.Lookup("error").Type()

// Analyzer is a linter that detects Err() methods that don't delegate
// to underlying types that also have Err() methods.
var Analyzer = &analysis.Analyzer{
	Name:     name,
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Map from type name to struct info
	structFields := make(map[*types.Named][]fieldWithErr)

	// First pass: find all struct types and their fields that have Err() methods
	inspect.Preorder([]ast.Node{(*ast.TypeSpec)(nil)}, func(n ast.Node) {
		typeSpec := n.(*ast.TypeSpec)
		structType, ok := typeSpec.Type.(*ast.StructType)
		if !ok {
			return
		}

		obj := pass.TypesInfo.Defs[typeSpec.Name]
		if obj == nil {
			return
		}
		named, ok := obj.Type().(*types.Named)
		if !ok {
			return
		}

		// Check each field for Err() method
		var fieldsWithErr []fieldWithErr
		if structType.Fields != nil {
			for _, field := range structType.Fields.List {
				fieldType := pass.TypesInfo.TypeOf(field.Type)
				if fieldType == nil {
					continue
				}

				// Check if this field's type has an Err() method
				if hasErrMethod(fieldType) {
					for _, name := range field.Names {
						fieldsWithErr = append(fieldsWithErr, fieldWithErr{
							name:      name.Name,
							fieldType: fieldType,
						})
					}
					// Handle embedded fields (no names)
					if len(field.Names) == 0 {
						// Get the type name for embedded field
						fieldName := getTypeName(fieldType)
						if fieldName != "" {
							fieldsWithErr = append(fieldsWithErr, fieldWithErr{
								name:      fieldName,
								fieldType: fieldType,
								embedded:  true,
							})
						}
					}
				}
			}
		}

		if len(fieldsWithErr) > 0 {
			structFields[named] = fieldsWithErr
		}
	})

	// Second pass: check Err() method implementations
	inspect.Preorder([]ast.Node{(*ast.FuncDecl)(nil)}, func(n ast.Node) {
		funcDecl := n.(*ast.FuncDecl)

		// Only interested in Err() error methods
		if funcDecl.Name.Name != "Err" {
			return
		}
		if funcDecl.Recv == nil || len(funcDecl.Recv.List) == 0 {
			return
		}
		if funcDecl.Type.Results == nil || len(funcDecl.Type.Results.List) != 1 {
			return
		}

		// Check if return type is error
		resultType := pass.TypesInfo.TypeOf(funcDecl.Type.Results.List[0].Type)
		if !types.Identical(resultType, errorType) {
			return
		}

		// Get the receiver type
		recvType := pass.TypesInfo.TypeOf(funcDecl.Recv.List[0].Type)
		if recvType == nil {
			return
		}

		// Dereference pointer if needed
		if ptr, ok := recvType.(*types.Pointer); ok {
			recvType = ptr.Elem()
		}

		named, ok := recvType.(*types.Named)
		if !ok {
			return
		}

		// Check if this type has fields with Err() methods
		fields, hasFieldsWithErr := structFields[named]
		if !hasFieldsWithErr {
			return
		}

		// Check if the Err() method body calls any of the underlying Err() methods
		if funcDecl.Body == nil {
			return
		}

		for _, field := range fields {
			if !callsFieldErr(funcDecl.Body, field.name) {
				if passesutil.HasNolintComment(pass, funcDecl, name) {
					continue
				}
				pass.Report(analysis.Diagnostic{
					Pos: funcDecl.Pos(),
					Message: "Err() method does not delegate to " + field.name + ".Err(); " +
						"errors from the underlying type may be lost. " +
						"Add \"//nolint:errdelegate\" to suppress if this is intentional.",
				})
			}
		}
	})

	return nil, nil
}

type fieldWithErr struct {
	name      string
	fieldType types.Type
	embedded  bool
}

// hasErrMethod checks if a type has an Err() error method.
func hasErrMethod(t types.Type) bool {
	// Dereference pointer types
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}

	// Look up Err method
	mset := types.NewMethodSet(t)
	for i := 0; i < mset.Len(); i++ {
		method := mset.At(i)
		if method.Obj().Name() != "Err" {
			continue
		}
		sig, ok := method.Obj().Type().(*types.Signature)
		if !ok {
			continue
		}
		// Check if it returns error
		if sig.Results().Len() == 1 && sig.Params().Len() == 0 {
			if types.Identical(sig.Results().At(0).Type(), errorType) {
				return true
			}
		}
	}

	// Also check pointer type method set
	ptrType := types.NewPointer(t)
	mset = types.NewMethodSet(ptrType)
	for i := 0; i < mset.Len(); i++ {
		method := mset.At(i)
		if method.Obj().Name() != "Err" {
			continue
		}
		sig, ok := method.Obj().Type().(*types.Signature)
		if !ok {
			continue
		}
		if sig.Results().Len() == 1 && sig.Params().Len() == 0 {
			if types.Identical(sig.Results().At(0).Type(), errorType) {
				return true
			}
		}
	}

	return false
}

// getTypeName gets the name of a type for embedded field access.
func getTypeName(t types.Type) string {
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	if named, ok := t.(*types.Named); ok {
		return named.Obj().Name()
	}
	return ""
}

// callsFieldErr checks if a function body contains a call to field.Err().
func callsFieldErr(body *ast.BlockStmt, fieldName string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		// Check for pattern: receiver.field.Err()
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if sel.Sel.Name != "Err" {
			return true
		}
		// Check if it's accessing the right field
		innerSel, ok := sel.X.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if innerSel.Sel.Name == fieldName {
			found = true
			return false
		}
		return true
	})
	return found
}
