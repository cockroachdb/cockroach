// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"reflect"

	"github.com/cockroachdb/errors"
)

// HintInjectionDonor holds the donor statement that provides the hints to be
// injected into a target statement. The donor statement could be a regular SQL
// statement or a statement fingerprint.
type HintInjectionDonor struct {
	// ast is the donor statement AST after parsing.
	ast Statement

	// validationSQL is the donor AST formatted such that we can safely compare it
	// with the target statement for validation. We'll only inject hints if the
	// statements match.
	validationSQL string

	// walk is a pre-order traversal of the AST, holding Expr, TableExpr, and
	// Statement nodes. As we walk the target AST we expect to find matching nodes
	// in the same order. Some nodes are ignored, such as ParenExpr and nodes
	// after collapse of a Tuple or Array or ValuesClause.
	walk []any
}

// NewHintInjectionDonor creates a HintInjectionDonor from a parsed AST. The
// parsed donor statement could be a regular SQL statement or a statement
// fingerprint.
//
// After NewHintInjectionDonor returns a HintInjectionDonor, the donor becomes
// read-only to allow concurrent use from multiple goroutines.
func NewHintInjectionDonor(ast Statement, fingerprintFlags FmtFlags) (*HintInjectionDonor, error) {
	hd := &HintInjectionDonor{
		ast:           ast,
		validationSQL: FormatStatementHideConstants(ast, fingerprintFlags, FmtHideHints),
	}
	v := newDonorVisitor{hd: hd, collapsed: []bool{false}}
	WalkStmt(&v, ast)
	return hd, nil
}

// newDonorVisitor is an ExtendedVisitor used to walk the donor AST and build
// the ahead-of-time pre-order traversal at donor creation time.
type newDonorVisitor struct {
	// hd is the hint donor being initialized.
	hd *HintInjectionDonor

	// collapsed is a stack indicating whether we've collapsed the rest of the
	// current ValuesClause, Tuple, or Array list in the donor AST.
	collapsed []bool
}

var _ ExtendedVisitor = &newDonorVisitor{}

func (v *newDonorVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if v.collapsed[len(v.collapsed)-1] {
		return false, expr
	}
	switch expr.(type) {
	case *ParenExpr:
		// We sometimes wrap scalar expressions in extra ParenExpr when
		// printing. Skip over ParenExpr to match in more cases.
		return true, expr
	case *Tuple, *Array:
		// If the donor was a statement fingerprint, we might have collapse a Tuple
		// or Array into __more__. Push on a boolean tracking whether we have
		// collapsed it.
		v.collapsed = append(v.collapsed, false)
	case *UnresolvedName:
		if isArityIndicatorString(expr.String()) {
			// If we find __more__ or another arity indicator, treat the rest of the
			// current Tuple or Array or ValuesClause as collapsed, and skip forward
			// until we VisitPost it.
			v.collapsed[len(v.collapsed)-1] = true
			v.hd.walk = append(v.hd.walk, expr)
			return false, expr
		}
	}
	v.hd.walk = append(v.hd.walk, expr)
	return true, expr
}

func (v *newDonorVisitor) VisitPost(expr Expr) (newNode Expr) {
	switch expr.(type) {
	case *Tuple, *Array:
		// Pop off the boolean tracking whether we collapsed the Tuple or Array.
		v.collapsed = v.collapsed[:len(v.collapsed)-1]
	}
	return expr
}

func (v *newDonorVisitor) VisitTablePre(expr TableExpr) (recurse bool, newExpr TableExpr) {
	if v.collapsed[len(v.collapsed)-1] {
		return false, expr
	}
	v.hd.walk = append(v.hd.walk, expr)
	return true, expr
}

func (v *newDonorVisitor) VisitTablePost(expr TableExpr) (newNode TableExpr) {
	return expr
}

func (v *newDonorVisitor) VisitStatementPre(expr Statement) (recurse bool, newNode Statement) {
	if v.collapsed[len(v.collapsed)-1] {
		return false, expr
	}
	switch expr.(type) {
	case *ValuesClause:
		// If the donor was a statement fingerprint, we might have collapse a
		// ValuesClause into __more__. Push on a boolean tracking whether we have
		// collapsed it.
		v.collapsed = append(v.collapsed, false)
	}
	v.hd.walk = append(v.hd.walk, expr)
	return true, expr
}

func (v *newDonorVisitor) VisitStatementPost(expr Statement) (newNode Statement) {
	switch expr.(type) {
	case *ValuesClause:
		// Pop off the boolean tracking whether we collapsed the ValuesClause.
		v.collapsed = v.collapsed[:len(v.collapsed)-1]
	}
	return expr
}

// hintInjectionVisitor is an ExtendedVisitor used to walk the target AST at
// injection time. It performs the rewrite of the target AST.
type hintInjectionVisitor struct {
	// hd is the hint donor. Note that multiple hintInjectionVisitors could share
	// the same donor concurrently from different sessions, so this is read-only.
	hd *HintInjectionDonor

	// walkIdx is the current position within the donor AST walk.
	walkIdx int

	// collapsed is a stack indicating whether we've collapsed the rest of the
	// current ValuesClause, Tuple, or Array list in the donor AST.
	collapsed []bool

	// err is set if we detect a mismatch while walking the target AST.
	err error
}

var _ ExtendedVisitor = &hintInjectionVisitor{}

func (v *hintInjectionVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if v.err != nil || v.collapsed[len(v.collapsed)-1] {
		return false, expr
	}

	switch expr.(type) {
	case *ParenExpr:
		// Skip ParenExpr to match the donor walk.
		return true, expr
	}

	if v.walkIdx >= len(v.hd.walk) {
		v.err = errors.Newf(
			"hint injection donor statement missing AST node corresponding to AST node %v", expr,
		)
		return false, expr
	}

	donorExpr := v.hd.walk[v.walkIdx]
	v.walkIdx += 1

	switch donor := donorExpr.(type) {
	case *Tuple, *Array:
		// If the donor was a statement fingerprint, we might have collapse a Tuple
		// or Array into __more__. Push on a boolean tracking whether we have
		// collapsed it.
		v.collapsed = append(v.collapsed, false)
	case *UnresolvedName:
		// If the donor was a fingerprint, then we might not exactly match _ or
		// __more__ or __more10_100__ etc in the donor. Treat these as wildcards
		// matching any subtree and don't recurse any further into the target AST.
		if donor.String() == string(StmtFingerprintPlaceholder) {
			return false, expr
		}
		if isArityIndicatorString(donor.String()) {
			// And skip the rest of the current Tuple or Array or ValuesClause if it
			// has been collapsed to __more__ or one of the other arity indicators.
			v.collapsed[len(v.collapsed)-1] = true
			return false, expr
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

func (v *hintInjectionVisitor) VisitPost(expr Expr) Expr {
	switch expr.(type) {
	case *Tuple, *Array:
		// Pop off the boolean tracking whether we collapsed the Tuple or Array.
		v.collapsed = v.collapsed[:len(v.collapsed)-1]
	}
	return expr
}

func (v *hintInjectionVisitor) VisitTablePre(expr TableExpr) (recurse bool, newExpr TableExpr) {
	if v.err != nil || v.collapsed[len(v.collapsed)-1] {
		return false, expr
	}

	if v.walkIdx >= len(v.hd.walk) {
		v.err = errors.Newf(
			"hint injection donor statement missing AST node corresponding to AST node", expr,
		)
		return false, expr
	}

	donorExpr := v.hd.walk[v.walkIdx]
	v.walkIdx += 1

	// Remove any existing hints from the original TableExpr, and copy hints from
	// the corresponding donor TableExpr.
	switch donor := donorExpr.(type) {
	case *AliasedTableExpr:
		switch orig := expr.(type) {
		case *AliasedTableExpr:
			if !donor.IndexFlags.Equal(orig.IndexFlags) {
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
			if donor.JoinType != orig.JoinType || donor.Hint != orig.Hint {
				// Create a new node with any pre-existing inline hints replaced with
				// hints from the donor.
				newNode := *orig
				newNode.JoinType = donor.JoinType
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

func (v *hintInjectionVisitor) VisitTablePost(expr TableExpr) TableExpr {
	return expr
}

func (v *hintInjectionVisitor) VisitStatementPre(expr Statement) (recurse bool, newExpr Statement) {
	if v.err != nil || v.collapsed[len(v.collapsed)-1] {
		return false, expr
	}

	if v.walkIdx >= len(v.hd.walk) {
		v.err = errors.Newf(
			"hint injection donor statement missing AST node corresponding to AST node %v", expr,
		)
		return false, expr
	}

	donorExpr := v.hd.walk[v.walkIdx]
	v.walkIdx += 1

	switch donorExpr.(type) {
	case *ValuesClause:
		// If the donor was a statement fingerprint, we might have collapse a
		// ValuesClause into __more__. Push on a boolean tracking whether we have
		// collapsed it.
		v.collapsed = append(v.collapsed, false)
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

func (v *hintInjectionVisitor) VisitStatementPost(expr Statement) Statement {
	switch expr.(type) {
	case *ValuesClause:
		// Pop off the boolean tracking whether we collapsed the ValuesClause.
		v.collapsed = v.collapsed[:len(v.collapsed)-1]
	}
	return expr
}

// Validate checks that the target statement exactly matches the donor (except
// for hints).
//
// It is safe to call Validate concurrently from multiple goroutines.
func (hd *HintInjectionDonor) Validate(stmt Statement, fingerprintFlags FmtFlags) error {
	sql := FormatStatementHideConstants(stmt, fingerprintFlags, FmtHideHints)
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
//
// It is safe to call InjectHints concurrently from multiple goroutines.
func (hd *HintInjectionDonor) InjectHints(stmt Statement) (Statement, bool, error) {
	// Walk the given statement, copying hints from the hints donor.
	v := hintInjectionVisitor{hd: hd, collapsed: []bool{false}}
	newStmt, changed := WalkStmt(&v, stmt)
	if v.err != nil {
		return stmt, false, v.err
	}
	if !changed {
		return stmt, false, nil
	}
	return newStmt, true, nil
}
