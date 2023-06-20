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
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

var _ scbuildstmt.TreeAnnotator = (*astAnnotator)(nil)

// astAnnotator creates a copy of the AST that can be annotated or modified
// safely without impacting the original statement.
type astAnnotator struct {
	statement        tree.Statement
	annotation       tree.Annotations
	nonExistentNames map[*tree.TableName]struct{}
}

func newAstAnnotator(original tree.Statement) (*astAnnotator, error) {
	// Clone the original tree by re-parsing the input back into an AST. We need
	// to keep tagged dollar quotes in case they're necessary to parse the
	// original statement.
	statement, err := parser.ParseOne(tree.AsStringWithFlags(original, tree.FmtTagDollarQuotes))
	if err != nil {
		return nil, err
	}
	// For `CREATE FUNCTION` stmt, overwrite `NumAnnotations` to be the largest of
	// parsing the node and parsing all its body statements, to avoid annotation
	// index-out-of-bound issue during formatting (https://github.com/cockroachdb/cockroach/issues/104242).
	if _, ok := statement.AST.(*tree.CreateFunction); ok {
		statement.NumAnnotations = numberAnnotationsOfCreateFunction(statement)
	}

	return &astAnnotator{
		nonExistentNames: map[*tree.TableName]struct{}{},
		statement:        statement.AST,
		annotation:       tree.MakeAnnotations(statement.NumAnnotations),
	}, nil
}

// GetStatement returns the cloned copy of the AST that is stored inside
// the annotator.
func (ann *astAnnotator) GetStatement() tree.Statement {
	return ann.statement
}

// GetAnnotations implements scbuildstmt.TreeAnnotator.
func (ann *astAnnotator) GetAnnotations() *tree.Annotations {
	// Sanity: Validate the annotations before returning them
	return &ann.annotation
}

// MarkNameAsNonExistent implements scbuildstmt.TreeAnnotator.
func (ann *astAnnotator) MarkNameAsNonExistent(name *tree.TableName) {
	ann.nonExistentNames[name] = struct{}{}
}

// SetUnresolvedNameAnnotation implements scbuildstmt.TreeAnnotator.
func (ann *astAnnotator) SetUnresolvedNameAnnotation(
	unresolvedName *tree.UnresolvedObjectName, annotation interface{},
) {
	unresolvedName.SetAnnotation(&ann.annotation, annotation)
}

// ValidateAnnotations validates if expected modifications have been applied
// on the AST. After the build phase all names should be fully resolved either
// by directly modifying the AST or through annotations.
func (ann *astAnnotator) ValidateAnnotations() {
	// Sanity: Goes through the entire AST and confirms that and table names that
	// appear inside it are fully resolved, meaning that both the database and
	// schema are known.
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

// numberAnnotationsOfCreateFunction parse each function body statement
// and return the largest NumAnnotations in `cfStmt` and in each of those
// parsed function body statement.
func numberAnnotationsOfCreateFunction(
	cfStmt statements.Statement[tree.Statement],
) tree.AnnotationIdx {
	if cf, ok := cfStmt.AST.(*tree.CreateFunction); !ok {
		panic(errors.AssertionFailedf("statement is not CREATE FUNCTION"))
	} else {
		var funcBodyFound bool
		var funcBodyStr string
		for _, option := range cf.Options {
			switch opt := option.(type) {
			case tree.FunctionBodyStr:
				funcBodyFound = true
				funcBodyStr = string(opt)
			}
		}
		if !funcBodyFound {
			panic(pgerror.New(pgcode.InvalidFunctionDefinition, "no function body specified"))
		}
		// ret = max(cfSmt.NumAnnotations, max(funcBodyStmt.NumAnnotations within funcBodyStmts))
		ret := cfStmt.NumAnnotations
		funcBodyStmts, err := parser.Parse(funcBodyStr)
		if err != nil {
			panic(err)
		}
		for _, funcBodyStmt := range funcBodyStmts {
			if funcBodyStmt.NumAnnotations > ret {
				ret = funcBodyStmt.NumAnnotations
			}
		}
		return ret
	}
}
