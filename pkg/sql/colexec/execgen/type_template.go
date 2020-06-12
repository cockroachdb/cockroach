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
	"go/token"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/dstutil"
)

// extractInstantiationsFromComment searches a node's comments for
// execgen:instantiate directives, returning the list of lists of instantiation
// arguments for the function.
// For example, a function with:
// execgen:instantiate<bool, int>
// execgen:instantiate<int, int>
// would get the result [][]string{{bool, int}, {int, int}}
func extractInstantiationsFromComment(n dst.Node) (instantiations [][]dst.Expr) {
	decs := n.Decorations()
	newDecs := dst.Decorations{}
	for _, dec := range decs.Start.All() {
		if matches := instantiateRe.FindStringSubmatch(dec); matches != nil {
			match := matches[1]
			// Match now looks like foo, bar
			var templateArgs []dst.Expr
			for _, v := range strings.Split(match, ",") {
				templateArgs = append(templateArgs, dst.NewIdent(strings.TrimSpace(v)))
			}
			instantiations = append(instantiations, templateArgs)
			continue
		}
		newDecs = append(newDecs, dec)
	}
	decs.Start = newDecs
	return instantiations
}

// createTemplateStructVariants finds all structs in the input file annotated
// with // execgen:template<T typename, U typename, ...> and produces variants
// for every nearby // execgen:instantiate<> annotation.
//
// It returns a map from struct type name to list of lists of instantiation
// arguments.
func createTemplateStructVariants(f *dst.File) {
	templateStructInfoMap := replaceTemplateStructs(f)

	dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		funcDecl, ok := n.(*dst.FuncDecl)
		if !ok {
			return true
		}
		if funcDecl.Recv == nil || len(funcDecl.Recv.List) == 0 {
			return true
		}
		recvType := funcDecl.Recv.List[0].Type
		switch n := recvType.(type) {
		case *dst.Ident:
			produceFuncDeclVariants(cursor, templateStructInfoMap, n.Name, funcDecl, false)
		case *dst.StarExpr:
			ident, ok := n.X.(*dst.Ident)
			if !ok {
				return true
			}
			produceFuncDeclVariants(cursor, templateStructInfoMap, ident.Name, funcDecl, true)
		}
		return true
	}, nil)
}

func produceFuncDeclVariants(
	cursor *dstutil.Cursor,
	templateStructInfoMap map[string]*templateStructInfo,
	name string,
	funcDecl *dst.FuncDecl,
	needStarExpr bool,
) {
	if info, ok := templateStructInfoMap[name]; ok {
		// At this point, we know we're looking at a method of a templated
		// struct. We want to generate variants for each of the instantiations
		// of the parent struct.
		for _, args := range info.instantiations {
			newMethod := dst.Clone(funcDecl).(*dst.FuncDecl)
			var typ dst.Expr = getTemplateVariantIdent(name, args)
			if needStarExpr {
				typ = &dst.StarExpr{X: typ}
			}
			newMethod.Recv.List[0].Type = typ
			replaceTypeVariables(newMethod, info.templateVarMap, args)
			cursor.InsertBefore(newMethod)
		}
		cursor.Delete()
	}
}

type templateStructInfo struct {
	templateVarMap map[string]int
	instantiations [][]dst.Expr
}

func replaceTemplateStructs(f *dst.File) map[string]*templateStructInfo {
	templateStructInfoMap := make(map[string]*templateStructInfo)
	dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		genDecl, ok := n.(*dst.GenDecl)
		if !ok {
			return true
		}
		if genDecl.Tok != token.TYPE {
			return true
		}
		typeSpec, ok := genDecl.Specs[0].(*dst.TypeSpec)
		if !ok {
			return true
		}
		structType, ok := typeSpec.Type.(*dst.StructType)
		if !ok {
			return true
		}

		// At this point, we definitely have a struct type declaration, so we can
		// go ahead and find its variants and generate them.

		// TODO(jordan): make sure to save the type variant info somewhere because
		// we'll need to use this exact same information when generating variants of
		// the struct's methods.

		templateVars := extractTemplateVarsFromComment(n)
		if templateVars == nil {
			return true
		}
		structInfo := &templateStructInfo{
			templateVarMap: templateVars,
		}
		structInfo.instantiations = extractInstantiationsFromComment(n)
		if structInfo.instantiations == nil {
			return true
		}

		// templateVars is now a list of the template variable strings.

		// Next, we want to generate a variant for every set of instantiation
		// arguments.

		for _, args := range structInfo.instantiations {
			// args is now the set of template type names.

			newTypeDecl := dst.Clone(structType).(*dst.StructType)
			replaceTypeVariables(newTypeDecl, structInfo.templateVarMap, args)

			// Produce mangled name for the struct.
			newName := getTemplateVariantIdent(typeSpec.Name.Name, args)

			cursor.InsertBefore(&dst.GenDecl{
				Tok: token.TYPE,
				Specs: []dst.Spec{
					&dst.TypeSpec{
						Name: newName,
						Type: newTypeDecl,
					},
				},
				Decs: genDecl.Decs,
			})
		}
		cursor.Delete()

		templateStructInfoMap[typeSpec.Name.Name] = structInfo
		return true
	}, nil)
	return templateStructInfoMap
}

// replaceTypeVariables replaces idents present in templateVarMap with the
// string argument in args at the index of their entry in the map.
func replaceTypeVariables(node dst.Node, templateVarMap map[string]int, args []dst.Expr) dst.Node {
	return dstutil.Apply(node, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		ident, ok := n.(*dst.Ident)
		if !ok {
			return true
		}
		if i, ok := templateVarMap[ident.Name]; ok {
			// We've found a spot to replace, replace it with the corresponding
			// template argument.
			ident.Name = prettyPrintExprs(args[i])
		}
		return true
	}, nil)
}
