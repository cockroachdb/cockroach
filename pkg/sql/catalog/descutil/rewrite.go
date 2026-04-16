// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descutil

import (
	"go/constant"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parserutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// RewriteTypeOIDs rewrites user-defined type OIDs in place.
func RewriteTypeOIDs(typ *types.T, rewriter catalog.DescriptorRewriteFn) error {
	if !typ.UserDefined() {
		return nil
	}
	id := catid.UserDefinedOIDToID(typ.Oid())
	newID, err := rewriter(id)
	if err != nil {
		return err
	}
	newOID := catid.TypeIDToOID(newID)

	arrayID := catid.UserDefinedOIDToID(typ.UserDefinedArrayOID())
	newArrayID, err := rewriter(arrayID)
	if err != nil {
		return err
	}
	newArrayOID := catid.TypeIDToOID(newArrayID)

	types.RemapUserDefinedTypeOIDs(typ, newOID, newArrayOID)
	if typ.Family() == types.ArrayFamily {
		return RewriteTypeOIDs(typ.ArrayContents(), rewriter)
	}
	return nil
}

// RewriteExprIDs rewrites type, sequence, and function OID references
// embedded in a SQL expression string.
func RewriteExprIDs(
	expr catpb.Expression, rewriter catalog.DescriptorRewriteFn,
) (catpb.Expression, error) {
	parsed, err := parserutils.ParseExpr(string(expr))
	if err != nil {
		return "", err
	}

	rewritten, err := rewriteTypeOIDsInNode(parsed, rewriter)
	if err != nil {
		return "", err
	}

	reparsed, err := parserutils.ParseExpr(rewritten)
	if err != nil {
		return "", err
	}

	seqRewritten, err := tree.SimpleVisit(reparsed, makeSeqRewriteFunc(rewriter))
	if err != nil {
		return "", err
	}
	funcRewritten, err := tree.SimpleVisit(seqRewritten, makeFuncRewriteFunc(rewriter))
	if err != nil {
		return "", err
	}

	return catpb.Expression(funcRewritten.String()), nil
}

// RewritePLpgSQLBodyIDs rewrites type OID and sequence references in a
// PL/pgSQL function body string.
func RewritePLpgSQLBodyIDs(
	body catpb.RoutineBody, rewriter catalog.DescriptorRewriteFn,
) (catpb.RoutineBody, error) {
	stmt, err := parserutils.PLpgSQLParse(string(body))
	if err != nil {
		return "", err
	}
	rewritten, err := rewriteTypeOIDsInNode(stmt.AST, rewriter)
	if err != nil {
		return "", err
	}
	reparsed, err := parserutils.PLpgSQLParse(rewritten)
	if err != nil {
		return "", err
	}
	visitor := plpgsqltree.SQLStmtVisitor{Fn: makeSeqRewriteFunc(rewriter)}
	newStmt := plpgsqltree.Walk(&visitor, reparsed.AST)
	if visitor.Err != nil {
		return "", visitor.Err
	}
	return catpb.RoutineBody(tree.AsString(newStmt)), nil
}

// RewriteViewQueryIDs rewrites type and sequence OID references in a
// view query string.
func RewriteViewQueryIDs(
	query catpb.Statement, rewriter catalog.DescriptorRewriteFn,
) (catpb.Statement, error) {
	stmt, err := parserutils.ParseOne(string(query))
	if err != nil {
		return "", err
	}

	rewritten, err := rewriteTypeOIDsInNode(stmt.AST, rewriter)
	if err != nil {
		return "", err
	}

	reparsed, err := parserutils.ParseOne(rewritten)
	if err != nil {
		return "", err
	}
	newStmt, err := tree.SimpleStmtVisit(reparsed.AST, makeSeqRewriteFunc(rewriter))
	if err != nil {
		return "", err
	}
	return catpb.Statement(newStmt.String()), nil
}

// rewriteTypeOIDsInNode rewrites type OID references in a formatted AST node.
func rewriteTypeOIDsInNode(
	node tree.NodeFormatter, rewriter catalog.DescriptorRewriteFn,
) (string, error) {
	var err error
	ctx := tree.NewFmtCtx(
		tree.FmtSerializable,
		tree.FmtIndexedTypeFormat(func(fmtCtx *tree.FmtCtx, ref *tree.OIDTypeReference) {
			if err != nil {
				fmtCtx.WriteString(ref.SQLString())
				return
			}
			id := catid.UserDefinedOIDToID(ref.OID)
			if descpb.IsVirtualTable(id) {
				id = descpb.ID(ref.OID)
			}
			newID, rewriteErr := rewriter(id)
			if rewriteErr != nil {
				err = rewriteErr
				fmtCtx.WriteString(ref.SQLString())
				return
			}
			newRef := &tree.OIDTypeReference{OID: catid.TypeIDToOID(newID)}
			fmtCtx.WriteString(newRef.SQLString())
		}),
	)
	ctx.FormatNode(node)
	formatted := ctx.CloseAndGetString()
	if err != nil {
		return "", err
	}
	return formatted, nil
}

func makeSeqRewriteFunc(
	rewriter catalog.DescriptorRewriteFn,
) func(expr tree.Expr) (bool, tree.Expr, error) {
	return func(expr tree.Expr) (bool, tree.Expr, error) {
		annotateTypeExpr, ok := expr.(*tree.AnnotateTypeExpr)
		if !ok {
			return true, expr, nil
		}
		typ, safe := tree.GetStaticallyKnownType(annotateTypeExpr.Type)
		if !safe || typ.Family() != types.OidFamily {
			return true, expr, nil
		}
		numVal, ok := annotateTypeExpr.Expr.(*tree.NumVal)
		if !ok {
			return true, expr, nil
		}
		seqID, err := numVal.AsInt64()
		if err != nil {
			// Not a valid sequence ID literal; skip this expression.
			return true, expr, nil //nolint:returnerrcheck
		}
		newID, err := rewriter(descpb.ID(seqID))
		if err != nil {
			return false, expr, err
		}
		annotateTypeExpr.Expr = tree.NewNumVal(
			constant.MakeInt64(int64(newID)),
			strconv.Itoa(int(newID)),
			false, /* negative */
		)
		return false, annotateTypeExpr, nil
	}
}

func makeFuncRewriteFunc(
	rewriter catalog.DescriptorRewriteFn,
) func(expr tree.Expr) (bool, tree.Expr, error) {
	return func(expr tree.Expr) (bool, tree.Expr, error) {
		funcExpr, ok := expr.(*tree.FuncExpr)
		if !ok {
			return true, expr, nil
		}
		oidRef, ok := funcExpr.Func.FunctionReference.(*tree.FunctionOID)
		if !ok {
			return true, expr, nil
		}
		if !catid.IsOIDUserDefined(oidRef.OID) {
			return true, expr, nil
		}
		fnID := catid.UserDefinedOIDToID(oidRef.OID)
		newID, err := rewriter(fnID)
		if err != nil {
			return false, expr, err
		}
		newFuncExpr := *funcExpr
		newFuncExpr.Func = tree.ResolvableFunctionReference{
			FunctionReference: &tree.FunctionOID{OID: catid.FuncIDToOID(newID)},
		}
		return true, &newFuncExpr, nil
	}
}
