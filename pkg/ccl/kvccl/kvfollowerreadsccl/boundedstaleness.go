// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfollowerreadsccl

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

func checkBoundedStalenessEnabled(evalCtx *eval.Context) error {
	return utilccl.CheckEnterpriseEnabled(
		evalCtx.Settings,
		evalCtx.ClusterID,
		"bounded staleness",
	)
}

func evalMaxStaleness(
	ctx context.Context, evalCtx *eval.Context, d duration.Duration,
) (time.Time, error) {
	if err := checkBoundedStalenessEnabled(evalCtx); err != nil {
		return time.Time{}, err
	}
	if d.Compare(duration.FromInt64(0)) < 0 {
		return time.Time{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"interval duration for %s must be greater or equal to 0",
			asof.WithMaxStalenessFunctionName,
		)
	}
	return duration.Add(evalCtx.GetStmtTimestamp(), d.Mul(-1)), nil
}

func evalMinTimestamp(ctx context.Context, evalCtx *eval.Context, t time.Time) (time.Time, error) {
	if err := checkBoundedStalenessEnabled(evalCtx); err != nil {
		return time.Time{}, err
	}
	t = t.Round(time.Microsecond)
	if stmtTimestamp := evalCtx.GetStmtTimestamp().Round(time.Microsecond); t.After(stmtTimestamp) {
		return time.Time{}, errors.WithDetailf(
			pgerror.Newf(
				pgcode.InvalidParameterValue,
				"timestamp for %s must be less than or equal to statement_timestamp()",
				asof.WithMinTimestampFunctionName,
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
