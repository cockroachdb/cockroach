// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type astAnnotator struct {
	statement        tree.Statement
	annotation       tree.Annotations
	nonExistentNames map[*tree.TableName]struct {
	}
}

func newAstAnnotator() *astAnnotator {
	return &astAnnotator{
		nonExistentNames: map[*tree.TableName]struct{}{},
	}
}

// SetStatementOnce implements scbuildstmt.AnnotationProvider
func (ann *astAnnotator) SetStatementOnce(original tree.Statement) tree.Statement {
	if ann.statement != nil {
		panic(errors.AssertionFailedf("AST should only be cloned once"))
	}
	// Clone the original tree by re-parsing the input back into an AST.
	statement, err := parser.ParseOne(original.String())
	if err != nil {
		panic(err)
	}
	ann.statement = statement.AST
	ann.annotation = tree.MakeAnnotations(statement.NumAnnotations)
	return ann.statement
}

// GetAnnotations implements scbuildstmt.AnnotationProvider
func (ann *astAnnotator) GetAnnotations() *tree.Annotations {
	return &ann.annotation
}

// MarkTableNameAsNonExistent implements scbuildstmt.AnnotationProvider
func (ann *astAnnotator) MarkTableNameAsNonExistent(name *tree.TableName) {
	ann.nonExistentNames[name] = struct{}{}
}

// ValidateAnnotations implements scbuildstmt.AnnotationProvider
func (ann *astAnnotator) ValidateAnnotations() {
	// Sanity: All the names are fully resolved inside the AST.
	f := tree.NewFmtCtx(
		tree.FmtAlwaysQualifyTableNames|tree.FmtMarkRedactionNode,
		tree.FmtAnnotations(&ann.annotation),
		tree.FmtReformatTableNames(func(ctx *tree.FmtCtx, name *tree.TableName) {
			// Name was not found during lookup inside the builder.
			if _, ok := ann.nonExistentNames[name]; ok {
				return
			}
			if name.CatalogName == "" || name.SchemaName == "" {
				panic(errors.AssertionFailedf("unresolved name inside annotated AST "+
					"(%v)", name.String()))
			}
		}))
	f.FormatNode(ann.statement)
}
