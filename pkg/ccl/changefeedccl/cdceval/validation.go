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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// ValidateSelectForTarget verifies that projections and filter expressions
// ara valid for a table and target family.  includeVirtual indicates if virtual columns
// should be considered valid in the expressions.
func ValidateSelectForTarget(
	ctx context.Context,
	st *cluster.Settings,
	sc sql.SpanConstrainer,
	evalCtx *eval.Context,
	codec keys.SQLCodec,
	desc catalog.TableDescriptor,
	target jobspb.ChangefeedTargetSpecification,
	selectClause *tree.SelectClause,
	includeVirtual bool,
) error {
	if !st.Version.IsActive(ctx, clusterversion.EnablePredicateProjectionChangefeed) {
		return errors.Newf(
			`filters and projections not supported until upgrade to version %s or higher is finalized`,
			clusterversion.EnablePredicateProjectionChangefeed.String())
	}

	ed, err := newEventDescriptorForTarget(desc, target, includeVirtual)
	if err != nil {
		return err
	}

	// Try to constrain spans by select clause.  We don't care about constrained spans here, but
	// constraining spans kicks off optimizer which detects many errors.
	if _, err := constrainSpansBySelectClause(ctx, sc, evalCtx, codec, selectClause, ed); err != nil {
		return err
	}

	evaluator, err := NewEvaluator(evalCtx, selectClause)
	if err != nil {
		return err
	}

	return evaluator.initEval(ctx, ed)
}

func newEventDescriptorForTarget(
	desc catalog.TableDescriptor, target jobspb.ChangefeedTargetSpecification, includeVirtual bool,
) (*cdcevent.EventDescriptor, error) {
	family, err := getTargetFamilyDescriptor(desc, target)
	if err != nil {
		return nil, err
	}
	return cdcevent.NewEventDescriptor(desc, family, includeVirtual, hlc.Timestamp{})
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
