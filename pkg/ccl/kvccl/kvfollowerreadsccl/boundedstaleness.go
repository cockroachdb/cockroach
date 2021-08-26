// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfollowerreadsccl

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

func checkBoundedStalenessEnabled(ctx *tree.EvalContext) error {
	st := ctx.Settings
	if !st.Version.IsActive(ctx.Ctx(), clusterversion.BoundedStaleness) {
		return pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			`bounded staleness reads requires all nodes to be upgraded to %s`,
			clusterversion.ByKey(clusterversion.BoundedStaleness),
		)
	}
	return utilccl.CheckEnterpriseEnabled(
		st,
		ctx.ClusterID,
		sql.ClusterOrganization.Get(&st.SV),
		"bounded staleness",
	)
}

func evalMaxStaleness(ctx *tree.EvalContext, d duration.Duration) (time.Time, error) {
	if err := checkBoundedStalenessEnabled(ctx); err != nil {
		return time.Time{}, err
	}
	if d.Compare(duration.FromInt64(0)) < 0 {
		return time.Time{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"interval duration for %s must be greater or equal to 0",
			tree.WithMaxStalenessFunctionName,
		)
	}
	return duration.Add(ctx.GetStmtTimestamp(), d.Mul(-1)), nil
}

func evalMinTimestamp(ctx *tree.EvalContext, t time.Time) (time.Time, error) {
	if err := checkBoundedStalenessEnabled(ctx); err != nil {
		return time.Time{}, err
	}
	t = t.Round(time.Microsecond)
	if stmtTimestamp := ctx.GetStmtTimestamp().Round(time.Microsecond); t.After(stmtTimestamp) {
		return time.Time{}, errors.WithDetailf(
			pgerror.Newf(
				pgcode.InvalidParameterValue,
				"timestamp for %s must be less than or equal to statement_timestamp()",
				tree.WithMinTimestampFunctionName,
			),
			"statement timestamp: %d, min_timestamp: %d",
			stmtTimestamp.UnixNano(),
			t.UnixNano(),
		)
	}
	return t, nil
}

func init() {
	builtins.WithMinTimestamp = evalMinTimestamp
	builtins.WithMaxStaleness = evalMaxStaleness
}
