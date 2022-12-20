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

// NormalizedSelectClause represents normalized and error checked cdc expression.
// Basically, it is a select clause returned by normalizeSelectClause.
// Methods on this expression modify the select clause in place, but this
// marker type is needed so that we can ensure functions that rely on
// normalized input aren't called out of order.
type NormalizedSelectClause struct {
	*tree.SelectClause
	desc *cdcevent.EventDescriptor
}

// RequiresPrev returns true if expression requires access to the previous
// version of the row.
func (n *NormalizedSelectClause) RequiresPrev() bool {
	return len(n.From.Tables) > 1
}

// SelectStatementForFamily returns tree.Select representing this object.
func (n *NormalizedSelectClause) SelectStatementForFamily() *tree.Select {
	if !n.desc.HasOtherFamilies {
		return &tree.Select{Select: n.SelectClause}
	}

	// Configure index flags to restrict access to specific column family.
	sc := *n.SelectClause
	sc.From.Tables = append(tree.TableExprs(nil), n.SelectClause.From.Tables...)
	sc.From.Tables[0] = &tree.AliasedTableExpr{
		Expr:       n.SelectClause.From.Tables[0],
		IndexFlags: &tree.IndexFlags{FamilyID: &n.desc.FamilyID},
	}

	return &tree.Select{Select: &sc}
}

// normalizeAndValidateSelectForTarget normalizes select expression and verifies
// expression is valid for a table and target family.
//
// The normalized (updated) select clause expression can be serialized into protocol
// buffer using cdceval.AsStringUnredacted.
// TODO(yevgeniy): Add support for virtual columns.
func normalizeAndValidateSelectForTarget(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	desc catalog.TableDescriptor,
	schemaTS hlc.Timestamp,
	target jobspb.ChangefeedTargetSpecification,
	sc *tree.SelectClause,
	keyOnly bool,
	splitColFams bool,
	semaCtx *tree.SemaContext,
) (_ *NormalizedSelectClause, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = errors.Newf("expression (%s) currently unsupported in CREATE CHANGEFEED: %s",
				tree.AsString(sc), r)
		}
	}()

	if !execCfg.Settings.Version.IsActive(ctx, clusterversion.V22_2EnablePredicateProjectionChangefeed) {
		return nil, errors.Newf(
			`filters and projections not supported until upgrade to version %s or higher is finalized`,
			clusterversion.V22_2EnablePredicateProjectionChangefeed.String())
	}

	// This really shouldn't happen as it's enforced by sql.y.
	if len(sc.From.Tables) != 1 {
		return nil, pgerror.Newf(pgcode.Syntax,
			"expected 1 table, found %d", len(sc.From.Tables))
	}

	// Sanity check target and descriptor refer to the same table.
	if target.TableID != desc.GetID() {
		return nil, errors.AssertionFailedf("target table id (%d) does not match descriptor id (%d)",
			target.TableID, desc.GetID())
	}

	columnVisitor := checkColumnsVisitor{
		desc:         desc,
		splitColFams: splitColFams,
	}
	err := columnVisitor.FindColumnFamilies(sc)
	if err != nil {
		return nil, err
	}
	target, err = getExpressionTargetSpecification(desc, target, &columnVisitor)
	if err != nil {
		return nil, err
	}

	// TODO(yevgeniy): support virtual columns.
	const includeVirtual = false
	d, err := newEventDescriptorForTarget(desc, target, schemaTS, includeVirtual, keyOnly)
	if err != nil {
		return nil, err
	}

	// Perform normalization.
	normalized, err := normalizeSelectClause(ctx, semaCtx, sc, d)
	if err != nil {
		return nil, err
	}

	return normalized, nil
}

func getExpressionTargetSpecification(
	desc catalog.TableDescriptor,
	target jobspb.ChangefeedTargetSpecification,
	cv *checkColumnsVisitor,
) (jobspb.ChangefeedTargetSpecification, error) {
	allFamilies := desc.GetFamilies()

	if target.FamilyName != "" {
		// Use target if family name set explicitly.
		return target, nil
	}

	if len(allFamilies) == 1 {
		// There is only 1 column family, so use that.
		target.Type = jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY
		return target, nil
	}

	keyColSet := desc.GetPrimaryIndex().CollectKeyColumnIDs()
	refColSet := catalog.MakeTableColSet(cv.columns...)
	nonKeyColSet := refColSet.Difference(keyColSet)
	numReferencedNonKeyFamilies := func() (ref int) {
		_ = desc.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
			if catalog.MakeTableColSet(family.ColumnIDs...).Intersects(nonKeyColSet) {
				ref++
			}
			return nil
		})
		return ref
	}()

	if cv.seenStar {
		if nonKeyColSet.Len() > 0 && numReferencedNonKeyFamilies > 1 {
			return target, pgerror.Newf(pgcode.InvalidParameterValue,
				"can't reference non-primary key columns as well as star on a multi column family table")
		}
		if !cv.splitColFams {
			return target, pgerror.Newf(pgcode.InvalidParameterValue,
				"targeting a table with multiple column families requires "+
					"WITH split_column_families and will emit multiple events per row.")
		}
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

	// If referenced families aren't being retrieved properly try using rowenc.NeededFamilyIDs
	var referencedFamilies []string
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
	if len(referencedFamilies) == 0 {
		return target, pgerror.Newf(
			pgcode.AssertFailure, "expression does not reference any column family")
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
	keyOnly bool,
) (*cdcevent.EventDescriptor, error) {
	family, err := getTargetFamilyDescriptor(desc, target)
	if err != nil {
		return nil, err
	}
	return cdcevent.NewEventDescriptor(desc, family, includeVirtual, keyOnly, schemaTS)
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

// normalizeSelectClause performs normalization step for select clause.
// Returns normalized select clause.
func normalizeSelectClause(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	sc *tree.SelectClause,
	desc *cdcevent.EventDescriptor,
) (*NormalizedSelectClause, error) {
	// Turn FROM clause to table reference.
	// Note: must specify AliasClause for TableRef expression; otherwise we
	// won't be able to deserialize string representation (grammar requires
	// "select ... from [table_id as tableAlias]")
	var tableAlias tree.AliasClause
	switch t := sc.From.Tables[0].(type) {
	case *tree.AliasedTableExpr:
		tableAlias = t.As
	case tree.TablePattern:
	case *tree.TableRef:
		tableAlias = t.As
	default:
		// This is verified by sql.y -- but be safe.
		return nil, errors.AssertionFailedf("unexpected table expression type %T",
			sc.From.Tables[0])
	}

	if tableAlias.Alias == "" {
		tableAlias.Alias = tree.Name(desc.TableName)
	}

	if tableAlias.Alias == prevTupleName {
		return nil, pgerror.Newf(pgcode.ReservedName,
			"%s is a reserved name in CDC; Specify different alias with AS clause", prevTupleName)
	}

	sc.From.Tables[0] = &tree.TableRef{
		TableID: int64(desc.TableID),
		As:      tableAlias,
	}

	// Setup sema ctx to handle cdc expressions. We want to make sure we only
	// override some properties, while keeping other properties (type resolver)
	// intact.
	defer configSemaForCDC(semaCtx, desc)()

	// Keep track of user defined types used in the expression.
	var udts map[oid.Oid]struct{}

	resolveType := func(ref tree.ResolvableTypeReference) (tree.ResolvableTypeReference, error) {
		typ, err := tree.ResolveType(ctx, ref, semaCtx.GetTypeResolver())
		if err != nil {
			return nil, pgerror.Wrapf(err, pgcode.IndeterminateDatatype,
				"could not resolve type %s", ref.SQLString())
		}

		if typ.UserDefined() {
			if udts == nil {
				udts = make(map[oid.Oid]struct{})
			}
			udts[typ.Oid()] = struct{}{}
		}
		return typ, nil
	}

	requiresPrev := false
	stmt, err := tree.SimpleStmtVisit(
		sc,
		func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
			// Replace type references with resolved type.
			switch e := expr.(type) {
			case *tree.AnnotateTypeExpr:
				typ, err := resolveType(e.Type)
				if err != nil {
					return false, expr, err
				}
				e.Type = typ
				return true, e, nil
			case *tree.CastExpr:
				typ, err := resolveType(e.Type)
				if err != nil {
					return false, expr, err
				}
				e.Type = typ
				return true, e, nil
			case *tree.FuncExpr:
				fn, err := checkFunctionSupported(ctx, e, semaCtx)
				if err != nil {
					return false, e, err
				}
				return true, fn, nil
			case *tree.Subquery:
				return false, e, pgerror.New(
					pgcode.FeatureNotSupported, "sub-query expressions not supported by CDC")
			case *tree.UnresolvedName:
				switch e.NumParts {
				case 1:
					if e.Parts[0] == prevTupleName {
						if _, err := desc.TableDescriptor().FindColumnWithName(prevTupleName); err == nil {
							return false, e,
								pgerror.Newf(pgcode.AmbiguousColumn,
									"ambiguous cdc_prev column collides with CDC reserved keyword.  "+
										"Disambiguate with %s.cdc_prev", desc.TableName)
						}

						requiresPrev = true
						return true, e, nil
					}

					// Qualify unqualified names.  Since we might be adding access to the
					// previous row, column names become ambiguous if they are not
					// qualified.
					return true, tree.NewUnresolvedName(string(tableAlias.Alias), e.Parts[0]), nil
				case 2:
					if e.Parts[1] == prevTupleName {
						requiresPrev = true
					}
				}
				return true, e, nil
			case tree.UnqualifiedStar:
				// Qualify unqualified stars.  Since we might be adding
				// access to the previous row, column names become ambiguous.
				return true, &tree.AllColumnsSelector{
					TableName: tree.NewUnqualifiedTableName(tableAlias.Alias).ToUnresolvedObjectName(),
				}, nil
			default:
				return true, expr, nil
			}
		})

	if err != nil {
		return nil, err
	}

	var norm *NormalizedSelectClause
	switch t := stmt.(type) {
	case *tree.SelectClause:
		if err := scopeAndRewrite(t, requiresPrev); err != nil {
			return nil, err
		}
		norm = &NormalizedSelectClause{
			SelectClause: t,
			desc:         desc,
		}
	default:
		// We walked tree.SelectClause -- getting anything else would be surprising.
		return nil, errors.AssertionFailedf("unexpected result type %T", stmt)
	}

	if len(udts) == 0 {
		return norm, nil
	}

	// Verify that the only user defined types used are the types referenced by
	// target table.
	allowedOIDs := make(map[oid.Oid]struct{})
	for _, c := range desc.TableDescriptor().UserDefinedTypeColumns() {
		allowedOIDs[c.GetType().Oid()] = struct{}{}
	}

	for id := range udts {
		if _, isAllowed := allowedOIDs[id]; !isAllowed {
			return nil, pgerror.Newf(pgcode.FeatureNotSupported,
				"use of user defined types not referenced by target table is not supported")
		}
	}

	return norm, nil
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

		c.columns = append(c.columns, col.GetID())
	case tree.UnqualifiedStar, *tree.AllColumnsSelector:
		c.seenStar = true
	}
	return true, expr
}

func (c *checkColumnsVisitor) FindColumnFamilies(sc *tree.SelectClause) error {
	_, err := tree.SimpleStmtVisit(sc, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		recurse, newExpr = c.VisitCols(expr)
		return recurse, newExpr, nil
	})
	return err
}

// scopeAndRewrite restricts this expression scope only to the columns
// being accessed, and rewrites select clause as needed to reflect that.
func scopeAndRewrite(sc *tree.SelectClause, requiresPrev bool) error {
	tables := append(tree.TableExprs(nil), sc.From.Tables...)
	if len(tables) != 1 {
		return errors.AssertionFailedf("expected single table")
	}

	if requiresPrev {
		// prevTupleTableExpr is a table expression to select contents of tuple
		// representing the previous row state.
		// That's a bit of a mouthful, but all we're doing here is adding
		// another table sub-select to the query to produce cdc_prev tuple:
		// SELECT ... FROM tbl, (SELECT ((crdb_internal.cdc_prev_row()).*)) AS cdc_prev
		// Note: even though this expression is the same for all queries, we should not
		// make it global because the underlying call (FunctionReference) to previous row
		// function will be replaced with function definition (concrete implementation).
		// Thus, if we reuse the same expression across different versions of event
		// descriptors, we will get unexpected errors.
		prevTupleTableExpr := &tree.AliasedTableExpr{
			As: tree.AliasClause{Alias: prevTupleName},
			Expr: &tree.Subquery{
				Select: &tree.ParenSelect{
					Select: &tree.Select{
						Select: &tree.SelectClause{
							Exprs: tree.SelectExprs{
								tree.SelectExpr{
									Expr: &tree.TupleStar{
										Expr: &tree.FuncExpr{
											Func: tree.ResolvableFunctionReference{FunctionReference: &prevRowFnName},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		tables = append(tables, prevTupleTableExpr)
	}

	sc.From = tree.From{Tables: tables}
	return nil
}
