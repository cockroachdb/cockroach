// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package parser parses Go files into an AST that can be easily formatted.
package parser

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
)

// File bundles an ast.File and a token.FileSet. It is more convenient to work
// with than its underlying objects when operating on only one file at a time.
type File struct {
	Decls []Decl

	file *ast.File
	fset *token.FileSet
	src  []byte
}

// ParseFile parses the source code of a single Go source file and returns the
// corresponding File. The name and src parameters operate as in
// go/parser.ParseFile.
func ParseFile(name string, src []byte) (*File, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, src, parser.AllErrors|parser.ParseComments)
	if err != nil {
		return nil, err
	}

	cr := commentListReader{fset: fset, comments: file.Comments}

	var decls []Decl
	for _, d := range file.Decls {
		if g, ok := d.(*ast.GenDecl); ok && g.Tok == token.IMPORT {
			impDecl := ImportDecl{
				Doc: g.Doc,
				Pos: g.Pos(),
				End: g.End(),
			}
			if g.Doc != nil {
				impDecl.Pos = g.Doc.Pos()
			}
			if !g.Lparen.IsValid() {
				impDecl.Doc = nil
				// Handling a non-block import with a line comment
				//
				//     import "foo" // comment
				//                ^
				// requires special care. g.End() will point to the end of the
				// import path, indicated with a caret in the diagram above. We
				// need to advance the end position to the end of the comment.
				if comment := g.Specs[0].(*ast.ImportSpec).Comment; comment != nil {
					impDecl.End = comment.End()
				}
			}
			for _, spec := range g.Specs {
				imp := *spec.(*ast.ImportSpec)
				if !g.Lparen.IsValid() {
					// Associate doc comments on non-block imports with the
					// import itself. This better matches most programmers'
					// mental model.
					imp.Doc = g.Doc
				}
				impDecl.Specs = append(impDecl.Specs, ImportSpec{imp})
			}
			decls = append(decls, &impDecl)
		} else if f, ok := d.(*ast.FuncDecl); ok {
			decls = append(decls, &FuncDecl{*f})

			if f.Type != nil {
				assocFieldListComments(&cr, f.Type.Params)
				if f.Type.Results != nil {
					assocFieldListComments(&cr, f.Type.Results)
				}
			}
		}
	}

	return &File{
		Decls: decls,
		file:  file,
		fset:  fset,
		src:   src,
	}, nil
}

// Position converts a token.Pos into a token.Position.
func (f *File) Position(p token.Pos) token.Position {
	return f.fset.Position(p)
}

// Pos converts a file offset into a token.Pos.
func (f *File) Pos(offset int) token.Pos {
	return f.fset.File(1).Pos(offset)
}

// Offset converts a token.Pos into a file offset.
func (f *File) Offset(p token.Pos) int {
	return f.Position(p).Offset
}

// Slice returns the bytes in the range [start, end).
func (f *File) Slice(start, end token.Pos) []byte {
	return f.src[f.Offset(start):f.Offset(end)]
}

// ImportDecl represents an import declaration in a File.
type ImportDecl struct {
	Doc      *ast.CommentGroup
	Specs    []ImportSpec
	Pos, End token.Pos
}

func (f *File) ImportDecls() []*ImportDecl {
	var out []*ImportDecl
	for _, d := range f.Decls {
		if impDecl, ok := d.(*ImportDecl); ok {
			out = append(out, impDecl)
		} else {
			// Imports are always first. If we see anything besides an import,
			// we're done.
			break
		}
	}
	return out
}

// ImportSpec represents an import specification in an ImportDecl.
type ImportSpec struct {
	ast.ImportSpec
}

// ImportSpecs returns all the imports specifications from all the import
// declarations in the file.
func (f *File) ImportSpecs() []ImportSpec {
	var out []ImportSpec
	for _, d := range f.ImportDecls() {
		out = append(out, d.Specs...)
	}
	return out
}

func (i *ImportSpec) Path() string {
	if t, err := strconv.Unquote(i.ImportSpec.Path.Value); err == nil {
		return t
	}
	return ""
}

type FuncDecl struct {
	ast.FuncDecl
}

// BodyEnd returns the end position of the function's body.
func (f *FuncDecl) BodyEnd() token.Pos {
	// f.Body is nil if the FuncDecl is a forward declaration.
	if f.Body == nil {
		return f.Type.End()
	}
	return f.Body.Pos() + 1
}

type Decl interface {
	decl()
}

func (i *ImportDecl) decl() {}
func (f *FuncDecl) decl()   {}
