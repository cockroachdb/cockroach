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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// RewriteFKs rewrites IDs in foreign key constraints.
func RewriteFKs(fks []descpb.ForeignKeyConstraint, rewriter catalog.DescriptorRewriteFn) error {
	for i := range fks {
		var err error
		fks[i].OriginTableID, err = rewriter(fks[i].OriginTableID)
		if err != nil {
			return err
		}
		fks[i].ReferencedTableID, err = rewriter(fks[i].ReferencedTableID)
		if err != nil {
			return err
		}
	}
	return nil
}

// RewriteMutations rewrites ID references in descriptor mutations.
func RewriteMutations(
	mutations []descpb.DescriptorMutation, rewriter catalog.DescriptorRewriteFn,
) error {
	for i := range mutations {
		if c := mutations[i].GetConstraint(); c != nil {
			switch c.ConstraintType {
			case descpb.ConstraintToUpdate_FOREIGN_KEY:
				var err error
				c.ForeignKey.OriginTableID, err = rewriter(c.ForeignKey.OriginTableID)
				if err != nil {
					return err
				}
				c.ForeignKey.ReferencedTableID, err = rewriter(c.ForeignKey.ReferencedTableID)
				if err != nil {
					return err
				}
			case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
				var err error
				c.UniqueWithoutIndexConstraint.TableID, err = rewriter(
					c.UniqueWithoutIndexConstraint.TableID)
				if err != nil {
					return err
				}
			case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
				// No descriptor ID references to rewrite.
			}
		}
		if col := mutations[i].GetColumn(); col != nil {
			if err := RewriteColumn(col, rewriter); err != nil {
				return err
			}
		}
	}
	return nil
}

// RewriteIDs rewrites a slice of descriptor IDs.
func RewriteIDs(ids []descpb.ID, rewriter catalog.DescriptorRewriteFn) error {
	for i, id := range ids {
		newID, err := rewriter(id)
		if err != nil {
			return err
		}
		ids[i] = newID
	}
	return nil
}

// RewriteDependedOnBy rewrites descriptor IDs in DependedOnBy back-references.
func RewriteDependedOnBy(
	refs []descpb.TableDescriptor_Reference, rewriter catalog.DescriptorRewriteFn,
) error {
	for i := range refs {
		var err error
		refs[i].ID, err = rewriter(refs[i].ID)
		if err != nil {
			return err
		}
	}
	return nil
}

// RewriteUniqueWithoutIndexConstraints rewrites table IDs in
// unique without index constraints.
func RewriteUniqueWithoutIndexConstraints(
	uwis []descpb.UniqueWithoutIndexConstraint, rewriter catalog.DescriptorRewriteFn,
) error {
	for i := range uwis {
		var err error
		uwis[i].TableID, err = rewriter(uwis[i].TableID)
		if err != nil {
			return err
		}
	}
	return nil
}

// RewriteColumn rewrites type OIDs and sequence/function references in a
// column descriptor.
func RewriteColumn(col *descpb.ColumnDescriptor, rewriter catalog.DescriptorRewriteFn) error {
	if err := rewriteTypeOIDs(col.Type, rewriter); err != nil {
		return err
	}
	if err := RewriteIDs(col.UsesSequenceIds, rewriter); err != nil {
		return err
	}
	if err := RewriteIDs(col.OwnsSequenceIds, rewriter); err != nil {
		return err
	}
	if err := RewriteIDs(col.UsesFunctionIds, rewriter); err != nil {
		return err
	}
	return nil
}

// rewriteTypeOIDs rewrites user-defined type OIDs in place.
func rewriteTypeOIDs(typ *types.T, rewriter catalog.DescriptorRewriteFn) error {
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
		return rewriteTypeOIDs(typ.ArrayContents(), rewriter)
	}
	return nil
}

// RewriteSequenceOwner rewrites the owner table ID in a sequence's
// ownership descriptor.
func RewriteSequenceOwner(
	owner *descpb.TableDescriptor_SequenceOpts_SequenceOwner, rewriter catalog.DescriptorRewriteFn,
) error {
	newID, err := rewriter(owner.OwnerTableID)
	if err != nil {
		return err
	}
	owner.OwnerTableID = newID
	return nil
}

// RewriteTriggers rewrites ID references in trigger descriptors.
func RewriteTriggers(
	triggers []descpb.TriggerDescriptor, rewriter catalog.DescriptorRewriteFn,
) error {
	for i := range triggers {
		var err error
		triggers[i].FuncID, err = rewriter(triggers[i].FuncID)
		if err != nil {
			return err
		}
		if err = RewriteIDs(triggers[i].DependsOn, rewriter); err != nil {
			return err
		}
		if err = RewriteIDs(triggers[i].DependsOnTypes, rewriter); err != nil {
			return err
		}
		if err = RewriteIDs(triggers[i].DependsOnRoutines, rewriter); err != nil {
			return err
		}
	}
	return nil
}

// RewritePolicies rewrites ID references in policy descriptors.
func RewritePolicies(
	policies []descpb.PolicyDescriptor, rewriter catalog.DescriptorRewriteFn,
) error {
	for i := range policies {
		if err := RewriteIDs(policies[i].DependsOnFunctions, rewriter); err != nil {
			return err
		}
		if err := RewriteIDs(policies[i].DependsOnTypes, rewriter); err != nil {
			return err
		}
		if err := RewriteIDs(policies[i].DependsOnRelations, rewriter); err != nil {
			return err
		}
	}
	return nil
}

// RewriteExprIDs rewrites type, sequence, and function OID references
// embedded in a SQL expression string.
func RewriteExprIDs(expr string, rewriter catalog.DescriptorRewriteFn) (string, error) {
	parsed, err := parserutils.ParseExpr(expr)
	if err != nil {
		return "", err
	}

	expr, err = rewriteTypeOIDsInNode(parsed, rewriter)
	if err != nil {
		return "", err
	}

	parsed, err = parserutils.ParseExpr(expr)
	if err != nil {
		return "", err
	}

	rewritten, err := tree.SimpleVisit(parsed, makeSeqReplaceFunc(rewriter))
	if err != nil {
		return "", err
	}
	rewritten, err = tree.SimpleVisit(rewritten, makeFuncReplaceFunc(rewriter))
	if err != nil {
		return "", err
	}

	return rewritten.String(), nil
}

// RewritePLpgSQLBodyIDs rewrites type OID and sequence references in a
// PL/pgSQL function body string.
func RewritePLpgSQLBodyIDs(body string, rewriter catalog.DescriptorRewriteFn) (string, error) {
	// Rewrite type OID references via formatting.
	stmt, err := parserutils.PLpgSQLParse(body)
	if err != nil {
		return "", err
	}
	body, err = rewriteTypeOIDsInNode(stmt.AST, rewriter)
	if err != nil {
		return "", err
	}
	// Rewrite sequence OID references via AST walking.
	stmt, err = parserutils.PLpgSQLParse(body)
	if err != nil {
		return "", err
	}
	v := plpgsqltree.SQLStmtVisitor{Fn: makeSeqReplaceFunc(rewriter)}
	newStmt := plpgsqltree.Walk(&v, stmt.AST)
	if v.Err != nil {
		return "", v.Err
	}
	return tree.AsString(newStmt), nil
}

// RewriteViewQueryIDs rewrites type and sequence OID references in a
// view query string.
func RewriteViewQueryIDs(query string, rewriter catalog.DescriptorRewriteFn) (string, error) {
	stmt, err := parserutils.ParseOne(query)
	if err != nil {
		return "", err
	}

	query, err = rewriteTypeOIDsInNode(stmt.AST, rewriter)
	if err != nil {
		return "", err
	}

	stmt, err = parserutils.ParseOne(query)
	if err != nil {
		return "", err
	}
	newStmt, err := tree.SimpleStmtVisit(stmt.AST, makeSeqReplaceFunc(rewriter))
	if err != nil {
		return "", err
	}
	return newStmt.String(), nil
}

// rewriteTypeOIDsInNode rewrites type OID references in a formatted AST node.
// The FmtIndexedTypeFormat callback doesn't support returning errors, so we
// capture the first error in a closure and check it after formatting.
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
	s := ctx.CloseAndGetString()
	if err != nil {
		return "", err
	}
	return s, nil
}

func makeSeqReplaceFunc(
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

func makeFuncReplaceFunc(
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

// RewriteSchemaChangerState rewrites descriptor IDs, type OIDs, and
// expression references in declarative schema changer state.
func RewriteSchemaChangerState(
	state *scpb.DescriptorState, rewriter catalog.DescriptorRewriteFn,
) error {
	for i := range state.Targets {
		t := &state.Targets[i]

		if err := screl.WalkDescIDs(t.Element(), func(id *descpb.ID) error {
			newID, err := rewriter(*id)
			if err != nil {
				return err
			}
			*id = newID
			return nil
		}); err != nil {
			return err
		}

		if err := screl.WalkExpressions(t.Element(), func(expr *catpb.Expression) error {
			if *expr == "" {
				return nil
			}
			newExpr, err := RewriteExprIDs(string(*expr), rewriter)
			if err != nil {
				return err
			}
			*expr = catpb.Expression(newExpr)
			return nil
		}); err != nil {
			return err
		}

		if err := screl.WalkTypes(t.Element(), func(t *types.T) error {
			return rewriteTypeOIDs(t, rewriter)
		}); err != nil {
			return err
		}
	}
	return nil
}
