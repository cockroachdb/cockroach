// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

// HintInjectionDonor holds the donor statement that provides the hints to be
// injected into a target statement.
type HintInjectionDonor struct {
	// ast is the donor statement AST after parsing.
	ast Statement

	// validationSQL is the donor AST formatted such that we can safely compare it
	// with the target statement for validation. We'll only inject hints if the
	// statements match.
	validationSQL string

	// walk is a pre-order traversal of the AST, holding Expr and TableExpr. As we
	// walk the target AST we expect to find matching nodes in the same order.
	walk []any
}

// validationFmtFlags returns the FmtFlags used to check that the donor
// statement and the target statement match. These should be the same FmtFlags
// used for statement fingerprints (including
// sql.stats.statement_fingerprint.format_mask) with FmtHideHints added.
func validationFmtFlags(sv *settings.Values) FmtFlags {
	stmtFingerprintFmtMask := FmtHideConstants | FmtFlags(QueryFormattingForFingerprintsMask.Get(sv))
	return stmtFingerprintFmtMask | FmtHideHints
}

// NewHintInjectionDonor creates a HintInjectionDonor from a parsed AST.
func NewHintInjectionDonor(ast Statement, sv *settings.Values) (*HintInjectionDonor, error) {
	donor := &HintInjectionDonor{
		ast:           ast,
		validationSQL: AsStringWithFlags(ast, validationFmtFlags(sv)),
	}
	if _, err := ExtendedSimpleStmtVisit(
		ast,
		func(expr Expr) (bool, Expr, error) {
			donor.walk = append(donor.walk, expr)
			return true, expr, nil
		},
		func(expr TableExpr) (bool, TableExpr, error) {
			donor.walk = append(donor.walk, expr)
			return true, expr, nil
		},
	); err != nil {
		return nil, err
	}
	return donor, nil
}

type hintInjectionVisitor struct {
	donor   *HintInjectionDonor
	walkIdx int
	err     error
}

func (v *hintInjectionVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if v.err != nil {
		return false, expr
	}

	if v.walkIdx >= len(v.donor.walk) {
		v.err = errors.Newf(
			"hint injection donor statement missing AST node corresponding to AST node %v", expr,
		)
		return false, expr
	}

	donorExpr := v.donor.walk[v.walkIdx]
	v.walkIdx += 1

	// Check that the corresponding donor Expr matches this node.
	if reflect.TypeOf(expr) != reflect.TypeOf(donorExpr) {
		v.err = errors.Newf(
			"hint injection donor statement AST node %v did not match AST node %v", donorExpr, expr,
		)
		return false, expr
	}
	return true, expr
}

func (v *hintInjectionVisitor) VisitTablePre(expr TableExpr) (recurse bool, newExpr TableExpr) {
	if v.err != nil {
		return false, expr
	}

	if v.walkIdx >= len(v.donor.walk) {
		v.err = errors.Newf(
			"hint injection donor statement missing AST node corresponding to AST node", expr,
		)
		return false, expr
	}

	donorExpr := v.donor.walk[v.walkIdx]
	v.walkIdx += 1

	// Copy hints from the corresponding donor TableExpr.
	switch donor := donorExpr.(type) {
	case *AliasedTableExpr:
		switch orig := expr.(type) {
		case *AliasedTableExpr:
			if donor.IndexFlags != nil {
				// Create a new node with any pre-existing inline hints replaced with
				// hints from the donor.
				newNode := *orig
				newNode.IndexFlags = donor.IndexFlags
				return true, &newNode
			}
			return true, expr
		case TableExpr:
			if donor.IndexFlags != nil {
				// Wrap the node in the donor hints.
				newNode := &AliasedTableExpr{
					Expr:       orig,
					IndexFlags: donor.IndexFlags,
				}
				return true, newNode
			}
			return false, expr
		}
	case *JoinTableExpr:
		switch orig := expr.(type) {
		case *JoinTableExpr:
			if donor.Hint != "" {
				// Create a new node with any pre-existing inline hints replaced with
				// hints from the donor.
				newNode := *orig
				newNode.Hint = donor.Hint
				return true, &newNode
			}
			return true, expr
		}
	}

	// Check that the corresponding donor Expr matches this node.
	if reflect.TypeOf(expr) != reflect.TypeOf(donorExpr) {
		v.err = errors.Newf(
			"hint injection donor statement AST node %v did not match AST node %v", donorExpr, expr,
		)
		return false, expr
	}
	return true, expr
}

func (v *hintInjectionVisitor) VisitPost(expr Expr) Expr                { return expr }
func (v *hintInjectionVisitor) VisitTablePost(expr TableExpr) TableExpr { return expr }

var _ ExtendedVisitor = &hintInjectionVisitor{}

// Validate checks that the target statement exactly matches the donor (except
// for hints).
func (hd *HintInjectionDonor) Validate(stmt Statement, sv *settings.Values) error {
	sql := AsStringWithFlags(stmt, validationFmtFlags(sv))
	if sql != hd.validationSQL {
		return errors.Newf(
			"statement does not match hint donor statement: %v vs %v", sql, hd.validationSQL,
		)
	}
	return nil
}

// InjectHints rewrites the target statement with hints from the donor. Hints
// from the donor replace inline hints in the target statement at corresponding
// AST nodes, but other inline hints in the target statement are kept.
//
// True is returned if hints were copied from the donor. False is returned if no
// hints were copied from the donor and no rewrite took place. If all donor
// hints already existed in the target statement, they are still rewritten and
// true is still returned.
//
// It is assumed that Validate was already called on the target statement.
//
// No semantic checks of the donor hints are performed. It is assumed that
// semantic checks happen elsewhere.
func (hd *HintInjectionDonor) InjectHints(stmt Statement) (Statement, bool, error) {
	// Walk the given statement, copying hints from the hints donor.
	v := hintInjectionVisitor{donor: hd}
	newStmt, changed := WalkStmt(&v, stmt)
	if v.err != nil {
		return stmt, false, v.err
	}
	if !changed {
		return stmt, false, nil
	}
	return newStmt, true, nil
}
