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
	"go/token"
	"regexp"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/dstutil"
)

type templateParamInfo struct {
	fieldOrdinal int
	field        *dst.Field
}

type funcInfo struct {
	decl           *dst.FuncDecl
	templateParams []templateParamInfo
}

// Match // execgen:template<foo, bar>
var templateRe = regexp.MustCompile(`\/\/ execgen:template<((?:(?:\w+),?\W*)+)>`)

// replaceTemplateVars removes the template arguments from a callsite of a
// templated function. It returns the template arguments that were used, and a
// new CallExpr that doesn't have the template arguments.
func replaceTemplateVars(
	info *funcInfo, call *dst.CallExpr,
) (templateArgs []dst.Expr, newCall *dst.CallExpr) {
	if len(info.templateParams) == 0 {
		return nil, call
	}
	templateArgs = make([]dst.Expr, len(info.templateParams))
	// Collect template arguments.
	for i, param := range info.templateParams {
		templateArgs[i] = dst.Clone(call.Args[param.fieldOrdinal]).(dst.Expr)
	}
	// Remove template vars from callsite.
	newArgs := make([]dst.Expr, 0, len(call.Args)-len(info.templateParams))
	for i := range call.Args {
		skip := false
		for _, p := range info.templateParams {
			if p.fieldOrdinal == i {
				skip = true
				break
			}
		}
		if !skip {
			newArgs = append(newArgs, call.Args[i])
		}
	}
	ret := dst.Clone(call).(*dst.CallExpr)
	ret.Args = newArgs
	return templateArgs, ret
}

// monomorphizeTemplate produces a variant of the input function body, given the
// definition of the function in funcInfo, and the concrete, template-time values
// that the function is being invoked with. It will try to find conditional
// statements that use the template variables and output only the branches that
// match.
//
// Currently, this only works on booleans :)
//
// For example, given the function:
// // execgen:inline
// // execgen:template<t>
// func b(t bool) int {
//   if t {
//     x = 3
//   } else {
//     x = 4
//   }
//   return x
// }
//
// and a caller
//   b(true)
// this function will generate
//   if true {
//     x = 3
//   } else {
//     x = 4
//   }
//   return x
//
// but because true is a constant, it will be reduced to
//
// x = 3
// return x
//
// Note that this method lexically replaces all formal parameters, so together
// with createTemplateFuncVariants, it enables templates to call other templates
// with template variables.
func monomorphizeTemplate(n dst.Node, info *funcInfo, args []dst.Expr) dst.Node {
	// Create map from formal param name to arg.
	paramMap := make(map[string]dst.Expr)
	for i, p := range info.templateParams {
		paramMap[p.field.Names[0].Name] = args[i]
	}
	n = dstutil.Apply(n, func(cursor *dstutil.Cursor) bool {
		// Replace all usages of the formal parameter with the template arg.
		c := cursor.Node()
		switch t := c.(type) {
		case *dst.Ident:
			if arg := paramMap[t.Name]; arg != nil {
				cursor.Replace(dst.Clone(arg))
			}
		}
		return true
	}, nil)

	return foldConditionals(n, info, args)
}

// foldConditionals edits conditional statements to try to remove branches that
// are statically falsifiable.
func foldConditionals(n dst.Node, info *funcInfo, args []dst.Expr) dst.Node {
	return dstutil.Apply(n, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.IfStmt:
			switch c := n.Cond.(type) {
			case *dst.Ident:
				if c.Name == "true" {
					newBody := foldConditionals(n.Body, info, args).(*dst.BlockStmt)
					for _, stmt := range newBody.List {
						cursor.InsertBefore(stmt)
					}
					cursor.Delete()
					return true
				}
				if c.Name == "false" {
					if n.Else != nil {
						newElse := foldConditionals(n.Else, info, args)
						switch e := newElse.(type) {
						case *dst.BlockStmt:
							for _, stmt := range e.List {
								cursor.InsertBefore(stmt)
							}
							cursor.Delete()
						default:
							cursor.Replace(newElse)
						}
					} else {
						cursor.Delete()
					}
				}
			}
		}
		return true
	}, nil)
}

// createTemplateFuncVariants, given an AST, finds all functions annotated with
// execgen:template<foo,bar>, and produces all of the necessary variants for
// every combination of possible values for the type of each template argument.
//
// For example, given a template function:
//
// // execgen:template<b>
// func foo (a int, b bool) {
//   if b {
//     return a
//   } else {
//     return a + 1
//   }
// }
//
// This function will add 2 new func decls to the AST:
//
// func foo_true(a int) {
//   return a
// }
//
// func foo_false(a int) {
//   return a + 1
// }
func createTemplateFuncVariants(f *dst.File) map[string]*funcInfo {
	ret := make(map[string]*funcInfo)

	dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.FuncDecl:
			var templateVars []string
			var templateDecPosition int
			for i, dec := range n.Decorations().Start.All() {
				if matches := templateRe.FindStringSubmatch(dec); matches != nil {
					match := matches[1]
					// Match now looks like foo, bar
					templateVars = strings.Split(match, ",")
					for i, v := range templateVars {
						templateVars[i] = strings.TrimSpace(v)
					}
					templateDecPosition = i
					break
				}
			}
			if templateVars == nil {
				return false
			}
			// Remove the template decoration.
			n.Decs.Start = append(
				n.Decs.Start[:templateDecPosition],
				n.Decs.Start[templateDecPosition+1:]...)

			// Process template funcs: find template params from runtime definition
			// and save in funcInfo.
			info := &funcInfo{}
			for _, v := range templateVars {
				var found bool
				for i, f := range n.Type.Params.List {
					// We can safely 0-index here because fields always have at least
					// one name, and we've already banned the case where they have more
					// than one. (e.g. func a (a int, b int, c, d int))
					if f.Names[0].Name == v {
						if ident, ok := f.Type.(*dst.Ident); !ok || ident.Name != "bool" {
							panic("can't currently handle non-boolean template variables :)")
						}
						info.templateParams = append(info.templateParams, templateParamInfo{
							fieldOrdinal: i,
							field:        dst.Clone(f).(*dst.Field),
						})
						found = true
						break
					}
				}
				if !found {
					panic(fmt.Errorf("template var %s not found", v))
				}
			}
			info.decl = n

			// Delete template params from runtime definition.
			newParamList := make([]*dst.Field, 0, len(n.Type.Params.List)-len(info.templateParams))
			for i, field := range n.Type.Params.List {
				var skip bool
				for _, p := range info.templateParams {
					if i == p.fieldOrdinal {
						skip = true
						break
					}
				}
				if !skip {
					newParamList = append(newParamList, field)
				}
			}
			n.Type.Params.List = newParamList

			// Now, make variants for every possible combination of allowed values of
			// the template variables.
			argsPossibilities := generateAllTemplateArgs(info.templateParams)

			funcDecs := info.decl.Decs
			// Replace the template function with a const marker, just so we can keep
			// the comments above the template function available.
			cursor.InsertBefore(&dst.GenDecl{
				Tok: token.CONST,
				Specs: []dst.Spec{
					&dst.ValueSpec{
						Names: []*dst.Ident{dst.NewIdent("_")},
						Values: []dst.Expr{
							&dst.BasicLit{
								Kind:  token.STRING,
								Value: fmt.Sprintf(`"template_%s"`, n.Name.Name),
							},
						},
					},
				},
				Decs: dst.GenDeclDecorations{
					NodeDecs: funcDecs.NodeDecs,
				},
			})

			// Extract the remaining execgen directives.
			directives := make([]string, 0)
			for _, s := range funcDecs.Start {
				if strings.HasPrefix(s, "// execgen") {
					directives = append(directives, s)
				}
			}

			for _, args := range argsPossibilities {
				newBody := monomorphizeTemplate(dst.Clone(n.Body).(*dst.BlockStmt), info, args).(*dst.BlockStmt)
				newName := getTemplateVariantName(info, args)
				cursor.InsertAfter(&dst.FuncDecl{
					Name: newName,
					Type: dst.Clone(info.decl.Type).(*dst.FuncType),
					Body: newBody,
					Decs: dst.FuncDeclDecorations{
						NodeDecs: dst.NodeDecs{
							Before: dst.EmptyLine,
							Start:  directives,
						},
					},
				})
			}
			ret[info.decl.Name.Name] = info
			cursor.Delete()
			return false
		}

		return true
	}, nil)
	return ret
}

func getTemplateVariantName(info *funcInfo, args []dst.Expr) *dst.Ident {
	var newName strings.Builder
	newName.WriteString(info.decl.Name.Name)
	for j := range args {
		newName.WriteByte('_')
		newName.WriteString(prettyPrintExprs(args[j]))
	}
	return dst.NewIdent(newName.String())
}

// generateAllTemplateArgs returns every possible combination of values for the
// list of parameter types passed in.
func generateAllTemplateArgs(paramInfos []templateParamInfo) [][]dst.Expr {
	if len(paramInfos) == 0 {
		return [][]dst.Expr{nil}
	}
	if ident, ok := paramInfos[0].field.Type.(*dst.Ident); !ok || ident.Name != "bool" {
		panic("can't deal with non-boolean template arguments right now")
	}

	inner := generateAllTemplateArgs(paramInfos[:len(paramInfos)-1])

	result := make([][]dst.Expr, 0)
	for _, b := range []bool{true, false} {
		for _, s := range inner {
			// s here should be the list of possible arguments of the first n-1 types.
			s = append(s[:], dst.NewIdent(fmt.Sprintf("%t", b)))
			result = append(result, s)
		}
	}

	return result
}

// replaceTemplateCallSites finds all CallExprs in the input AST that are calling
// the functions that had been annotated with // execgen:template that are
// passed in via the templateFuncInfos map.
func replaceTemplateCallSites(f *dst.File, templateFuncInfos map[string]*funcInfo) dst.Node {
	return dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.CallExpr:
			ident, ok := n.Fun.(*dst.Ident)
			if !ok {
				return true
			}
			info, ok := templateFuncInfos[ident.Name]
			if !ok {
				// Nothing to do, it's not a templated function.
				return true
			}
			templateArgs, newCall := replaceTemplateVars(info, n)
			newCall.Fun = getTemplateVariantName(info, templateArgs)
			cursor.Replace(newCall)
			return false
		}
		return true
	}, nil)
}

// expandTemplates is the main entry point to the templater. Given a dst.File,
// it modifies the dst.File to include all expanded template functions, and
// edits call sites to call the newly expanded functions.
func expandTemplates(f *dst.File) {
	funcInfos := createTemplateFuncVariants(f)
	replaceTemplateCallSites(f, funcInfos)
}
