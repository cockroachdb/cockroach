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
) error {
	execCtx.SemaCtx()
	execCfg := execCtx.ExecCfg()
	if !execCfg.Settings.Version.IsActive(ctx, clusterversion.EnablePredicateProjectionChangefeed) {
		return errors.Newf(
			`filters and projections not supported until upgrade to version %s or higher is finalized`,
			clusterversion.EnablePredicateProjectionChangefeed.String())
	}

	// This really shouldn't happen as it's enforced by sql.y.
	if len(sc.From.Tables) != 1 {
		return pgerror.Newf(pgcode.Syntax, "invalid CDC expression: only 1 table supported")
	}

	// Sanity check target and descriptor refer to the same table.
	if target.TableID != desc.GetID() {
		return errors.AssertionFailedf("target table id (%d) does not match descriptor id (%d)",
			target.TableID, desc.GetID())
	}

	// This method is meant to be called early on when changefeed is created --
	// i.e. during planning. As such, we expect execution context to have
	// associated Txn() -- without which we cannot perform normalization.  Verify
	// this assumption (txn is needed for type resolution).
	if execCtx.Txn() == nil {
		return errors.AssertionFailedf("expected non-nil transaction")
	}

	// Perform normalization.
	if err := normalizeSelectClause(ctx, *execCtx.SemaCtx(), sc, desc); err != nil {
		return err
	}

	ed, err := newEventDescriptorForTarget(desc, target, schemaTS(execCtx), includeVirtual)
	if err != nil {
		return err
	}

	evalCtx := &execCtx.ExtendedEvalContext().Context
	// Try to constrain spans by select clause.  We don't care about constrained
	// spans here, but constraining spans kicks off optimizer which detects many
	// errors.
	if _, _, err := constrainSpansBySelectClause(
		ctx, execCtx, evalCtx, execCfg.Codec, sc, ed,
	); err != nil {
		return err
	}

	// Construct and initialize evaluator.  This performs some static checks,
	// and (importantly) type checks expressions.
	evaluator, err := NewEvaluator(evalCtx, sc)
	if err != nil {
		return err
	}

	return evaluator.initEval(ctx, ed)
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
			"projections and filter cannot be used when running against multifamily table (table has %d families)",
			desc.NumFamilies())
	default:
		return nil, errors.AssertionFailedf("invalid target type %v", target.Type)
	}
}

// normalizeSelectClause performs normalization step for select clause.
// Passed in select clause modified in place.
func normalizeSelectClause(
	ctx context.Context,
	semaCtx tree.SemaContext,
	sc *tree.SelectClause,
	desc catalog.TableDescriptor,
) error {
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
		return errors.AssertionFailedf("unexpected table expression type %T",
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
	semaCtx.SearchPath = &cdcCustomFunctionResolver{SearchPath: semaCtx.SearchPath}
	semaCtx.Properties.Require("cdc", rejectInvalidCDCExprs)

	resolveType := func(ref *tree.ResolvableTypeReference) error {
		typ, err := tree.ResolveType(ctx, *ref, semaCtx.GetTypeResolver())
		if err != nil {
			return pgerror.Wrapf(err, pgcode.IndeterminateDatatype,
				"could not resolve type %s", (*ref).SQLString())
		}
		*ref = &tree.OIDTypeReference{OID: typ.Oid()}
		return nil
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
			if err := resolveType(&e.Type); err != nil {
				return false, expr, err
			}
		case *tree.CastExpr:
			if err := resolveType(&e.Type); err != nil {
				return false, expr, err
			}
		}

		// Collect resolved type OIDs.
		recurse, newExpr = v.VisitPre(expr)
		return recurse, newExpr, nil
	})

	if err != nil {
		return err
	}
	sc = stmt.(*tree.SelectClause)

	if len(v.OIDs) == 0 {
		return nil
	}

	// Verify that the only user defined types used are the types referenced by
	// target table.
	allowedOIDs := make(map[oid.Oid]struct{})
	for _, c := range desc.UserDefinedTypeColumns() {
		allowedOIDs[c.GetType().Oid()] = struct{}{}
	}

	for id := range v.OIDs {
		if _, isAllowed := allowedOIDs[id]; !isAllowed {
			return pgerror.Newf(pgcode.FeatureNotSupported,
				"use of user defined types not references by target table is not supported")
		}
	}

	return nil
}
