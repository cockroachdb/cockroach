// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// NormalizeAndValidateSelectForTarget normalizes select expression and verifies
// expression is valid for a table and target family.  includeVirtual indicates
// if virtual columns should be considered valid in the expressions.
// Normalization steps include:
//   * Table name replaces with table reference
//   * UDTs values replaced with their physical representation (to keep expression stable
//     across data type changes).
// The normalized (updated) select clause expression can be serialized into protocol
// buffer using cdceval.AsStringUnredacted.
func NormalizeAndValidateSelectForTarget(
	ctx context.Context,
	execCtx sql.JobExecContext,
	desc catalog.TableDescriptor,
	target jobspb.ChangefeedTargetSpecification,
	sc *tree.SelectClause,
	includeVirtual bool,
	splitColFams bool,
) (n NormalizedSelectClause, _ jobspb.ChangefeedTargetSpecification, _ error) {
	execCtx.SemaCtx()
	execCfg := execCtx.ExecCfg()
	if !execCfg.Settings.Version.IsActive(ctx, clusterversion.EnablePredicateProjectionChangefeed) {
		return n, target, errors.Newf(
			`filters and projections not supported until upgrade to version %s or higher is finalized`,
			clusterversion.EnablePredicateProjectionChangefeed.String())
	}

	// This really shouldn't happen as it's enforced by sql.y.
	if len(sc.From.Tables) != 1 {
		return n, target, pgerror.Newf(pgcode.Syntax, "invalid CDC expression: only 1 table supported")
	}

	// Sanity check target and descriptor refer to the same table.
	if target.TableID != desc.GetID() {
		return n, target, errors.AssertionFailedf("target table id (%d) does not match descriptor id (%d)",
			target.TableID, desc.GetID())
	}

	// This method is meant to be called early on when changefeed is created --
	// i.e. during planning. As such, we expect execution context to have
	// associated Txn() -- without which we cannot perform normalization.  Verify
	// this assumption (txn is needed for type resolution).
	if execCtx.Txn() == nil {
		return n, target, errors.AssertionFailedf("expected non-nil transaction")
	}

	// Perform normalization.
	var err error
	normalized, err := normalizeSelectClause(ctx, *execCtx.SemaCtx(), sc, desc)
	if err != nil {
		return n, target, err
	}

	columnVisitor := checkColumnsVisitor{
		desc:         desc,
		splitColFams: splitColFams,
	}

	err = columnVisitor.FindColumnFamilies(normalized)
	if err != nil {
		return n, target, err
	}

	target, err = setTargetType(desc, target, &columnVisitor)
	if err != nil {
		return n, target, err
	}

	ed, err := newEventDescriptorForTarget(desc, target, schemaTS(execCtx), includeVirtual)
	if err != nil {
		return n, target, err
	}

	evalCtx := &execCtx.ExtendedEvalContext().Context
	// Try to constrain spans by select clause.  We don't care about constrained
	// spans here, but constraining spans kicks off optimizer which detects many
	// errors.
	if _, _, err := constrainSpansBySelectClause(
		ctx, execCtx, evalCtx, execCfg.Codec, sc, ed,
	); err != nil {
		return n, target, err
	}

	// Construct and initialize evaluator.  This performs some static checks,
	// and (importantly) type checks expressions.
	evaluator, err := NewEvaluator(evalCtx, sc)
	if err != nil {
		return n, target, err
	}

	return normalized, target, evaluator.initEval(ctx, ed)
}

func setTargetType(
	desc catalog.TableDescriptor,
	target jobspb.ChangefeedTargetSpecification,
	cv *checkColumnsVisitor,
) (jobspb.ChangefeedTargetSpecification, error) {
	allFamilies := desc.GetFamilies()

	if len(allFamilies) == 1 {
		target.Type = jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY
		return target, nil
	}

	var referencedFamilies []string

	keyColSet := desc.GetPrimaryIndex().CollectKeyColumnIDs()
	refColSet := catalog.MakeTableColSet(cv.columns...)
	nonKeyColSet := refColSet.Difference(keyColSet)

	if cv.seenStar {
		if nonKeyColSet.Len() > 0 {
			return target, pgerror.Newf(pgcode.InvalidParameterValue, "can't reference non-primary key columns as well as star on a multi column family table")
		}
		if cv.splitColFams {
			target.Type = jobspb.ChangefeedTargetSpecification_EACH_FAMILY
			return target, pgerror.Newf(pgcode.FeatureNotSupported,
				"split_column_families is not supported with changefeed expressions yet")
		}
		return target, pgerror.Newf(pgcode.InvalidParameterValue, "targeting a table with multiple column families requires WITH split_column_families and will emit multiple events per row.")
	}

	// If no non-primary key columns are being referenced, then we can assume that if
	// code has reached this point, only key columns are being referenced in the
	// SELECT statement. This may lead to weird behavior as primary keys are a part
	// of every column family technically. To handle this, we will assign the target
	// family to be the column family that the first primary key is in.
	if nonKeyColSet.Len() == 0 {
		target.Type = jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY
		for _, family := range allFamilies {
			famColSet := catalog.MakeTableColSet(family.ColumnIDs...)
			if famColSet.Contains(desc.GetPrimaryIndex().GetKeyColumnID(0)) {
				target.FamilyName = family.Name
				return target, nil
			}
		}
	}

	// If referenced families aren't being retrived properly try using rowenc.NeededFamilyIDs
	for _, family := range allFamilies {
		famColSet := catalog.MakeTableColSet(family.ColumnIDs...)
		if nonKeyColSet.Intersects(famColSet) {
			referencedFamilies = append(referencedFamilies, family.Name)
		}
	}

	if len(referencedFamilies) > 1 {
		target.Type = jobspb.ChangefeedTargetSpecification_EACH_FAMILY
		return target, pgerror.Newf(pgcode.InvalidParameterValue,
			"expressions can't reference columns from more than one column family")
	}
	target.Type = jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY
	target.FamilyName = referencedFamilies[0]
	return target, nil
}

func newEventDescriptorForTarget(
	desc catalog.TableDescriptor,
	target jobspb.ChangefeedTargetSpecification,
	schemaTS hlc.Timestamp,
	includeVirtual bool,
) (*cdcevent.EventDescriptor, error) {
	family, err := getTargetFamilyDescriptor(desc, target)
	if err != nil {
		return nil, err
	}
	return cdcevent.NewEventDescriptor(desc, family, includeVirtual, schemaTS)
}

func getTargetFamilyDescriptor(
	desc catalog.TableDescriptor, target jobspb.ChangefeedTargetSpecification,
) (*descpb.ColumnFamilyDescriptor, error) {
	switch target.Type {
	case jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY:
		return desc.FindFamilyByID(0)
	case jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY:
		var fd *descpb.ColumnFamilyDescriptor
		for _, family := range desc.GetFamilies() {
			if family.Name == target.FamilyName {
				fd = &family
				break
			}
		}
		if fd == nil {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue, "no such family %s", target.FamilyName)
		}
		return fd, nil
	case jobspb.ChangefeedTargetSpecification_EACH_FAMILY:
		// TODO(yevgeniy): Relax this restriction; some predicates/projectsion
		// are entirely fine to use (e.g "*").
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"expressions can't reference columns from more than one column family")
	default:
		return nil, errors.AssertionFailedf("invalid target type %v", target.Type)
	}
}

// NormalizedSelectClause is a select clause returned by normalizeSelectClause.
// normalizeSelectClause also modifies the select clause in place, but this
// marker type is needed so that we can ensure functions that rely on
// normalized input aren't called out of order.
type NormalizedSelectClause tree.SelectClause

// Clause returns a pointer to the underlying SelectClause (still in normalized
// form).
func (n NormalizedSelectClause) Clause() *tree.SelectClause {
	sc := tree.SelectClause(n)
	return &sc
}

// normalizeSelectClause performs normalization step for select clause.
// Returns normalized select clause.
func normalizeSelectClause(
	ctx context.Context,
	semaCtx tree.SemaContext,
	sc *tree.SelectClause,
	desc catalog.TableDescriptor,
) (normalizedSelectClause NormalizedSelectClause, _ error) {
	// Turn FROM clause to table reference.
	// Note: must specify AliasClause for TableRef expression; otherwise we
	// won't be able to deserialize string representation (grammar requires
	// "select ... from [table_id as alias]")
	var alias tree.AliasClause
	switch t := sc.From.Tables[0].(type) {
	case *tree.AliasedTableExpr:
		alias = t.As
	case tree.TablePattern:
	default:
		// This is verified by sql.y -- but be safe.
		return normalizedSelectClause, errors.AssertionFailedf("unexpected table expression type %T",
			sc.From.Tables[0])
	}

	if alias.Alias == "" {
		alias.Alias = tree.Name(desc.GetName())
	}
	sc.From.Tables[0] = &tree.TableRef{
		TableID: int64(desc.GetID()),
		As:      alias,
	}

	// Setup sema ctx to handle cdc expressions. We want to make sure we only
	// override some properties, while keeping other properties (type resolver)
	// intact.
	semaCtx.FunctionResolver = &CDCFunctionResolver{}
	semaCtx.Properties.Require("cdc", rejectInvalidCDCExprs)

	resolveType := func(ref tree.ResolvableTypeReference) (tree.ResolvableTypeReference, bool, error) {
		typ, err := tree.ResolveType(ctx, ref, semaCtx.GetTypeResolver())
		if err != nil {
			return nil, false, pgerror.Wrapf(err, pgcode.IndeterminateDatatype,
				"could not resolve type %s", ref.SQLString())
		}
		return &tree.OIDTypeReference{OID: typ.Oid()}, typ.UserDefined(), nil
	}

	// Verify that any UDTs used in the statement reference only the UDTs that are
	// part of the target table descriptor.
	v := &tree.TypeCollectorVisitor{
		OIDs: make(map[oid.Oid]struct{}),
	}

	stmt, err := tree.SimpleStmtVisit(sc, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		// Replace type references with resolved type.
		switch e := expr.(type) {
		case *tree.AnnotateTypeExpr:
			typ, udt, err := resolveType(e.Type)
			if err != nil {
				return false, expr, err
			}
			if !udt {
				// Only care about user defined types.
				return true, expr, nil
			}
			e.Type = typ

		case *tree.CastExpr:
			typ, udt, err := resolveType(e.Type)
			if err != nil {
				return false, expr, err
			}
			if !udt {
				// Only care about user defined types.
				return true, expr, nil
			}

			e.Type = typ
		}

		// Collect resolved type OIDs.
		recurse, newExpr = v.VisitPre(expr)
		return recurse, newExpr, nil
	})

	if err != nil {
		return normalizedSelectClause, err
	}
	switch t := stmt.(type) {
	case *tree.SelectClause:
		normalizedSelectClause = NormalizedSelectClause(*t)
	default:
		// We walked tree.SelectClause -- getting anything else would be surprising.
		return normalizedSelectClause, errors.AssertionFailedf("unexpected result type %T", stmt)
	}

	if len(v.OIDs) == 0 {
		return normalizedSelectClause, nil
	}

	// Verify that the only user defined types used are the types referenced by
	// target table.
	allowedOIDs := make(map[oid.Oid]struct{})
	for _, c := range desc.UserDefinedTypeColumns() {
		allowedOIDs[c.GetType().Oid()] = struct{}{}
	}

	for id := range v.OIDs {
		if _, isAllowed := allowedOIDs[id]; !isAllowed {
			return normalizedSelectClause, pgerror.Newf(pgcode.FeatureNotSupported,
				"use of user defined types not referenced by target table is not supported")
		}
	}

	return normalizedSelectClause, nil
}

type checkForPrevVisitor struct {
	semaCtx   tree.SemaContext
	ctx       context.Context
	foundPrev bool
}

// VisitPre implements the Visitor interface.
func (v *checkForPrevVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	if exprRequiresPreviousValue(v.ctx, v.semaCtx, expr) {
		v.foundPrev = true
		// no need to keep recursing
		return false, expr
	}
	return true, expr
}

// VisitPost implements the Visitor interface.
func (v *checkForPrevVisitor) VisitPost(e tree.Expr) tree.Expr {
	return e
}

// exprRequiresPreviousValue returns true if the top-level expression
// is a function call that cdc implements using the diff from a rangefeed.
func exprRequiresPreviousValue(ctx context.Context, semaCtx tree.SemaContext, e tree.Expr) bool {
	if f, ok := e.(*tree.FuncExpr); ok {
		funcDef, err := f.Func.Resolve(ctx, semaCtx.SearchPath, semaCtx.FunctionResolver)
		if err != nil {
			return false
		}
		return funcDef.Name == "cdc_prev"
	}
	return false
}

// SelectClauseRequiresPrev checks whether a changefeed expression will need a row's previous values
// to be fetched in order to evaluate it.
func SelectClauseRequiresPrev(
	ctx context.Context, semaCtx tree.SemaContext, sc NormalizedSelectClause,
) (bool, error) {
	c := checkForPrevVisitor{semaCtx: semaCtx, ctx: ctx}
	_, err := tree.SimpleStmtVisit(sc.Clause(), func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		recurse, newExpr = c.VisitPre(expr)
		return recurse, newExpr, nil
	})
	return c.foundPrev, err
}

type checkColumnsVisitor struct {
	err          error
	desc         catalog.TableDescriptor
	columns      []descpb.ColumnID
	seenStar     bool
	splitColFams bool
}

func (c *checkColumnsVisitor) VisitCols(expr tree.Expr) (bool, tree.Expr) {
	switch e := expr.(type) {
	case *tree.UnresolvedName:
		vn, err := e.NormalizeVarName()
		if err != nil {
			c.err = err
			return false, expr
		}
		return c.VisitCols(vn)

	case *tree.ColumnItem:
		col, err := c.desc.FindColumnWithName(e.ColumnName)
		if err != nil {
			c.err = err
			return false, expr
		}
		colID := col.GetID()
		c.columns = append(c.columns, colID)

	case tree.UnqualifiedStar:
		c.seenStar = true
	}
	return true, expr
}

func (c *checkColumnsVisitor) FindColumnFamilies(sc NormalizedSelectClause) error {
	_, err := tree.SimpleStmtVisit(sc.Clause(), func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		recurse, newExpr = c.VisitCols(expr)
		return recurse, newExpr, nil
	})
	return err
}
