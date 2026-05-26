// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package followerreads

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

// EvalMaxStaleness implements the with_max_staleness builtin.
func EvalMaxStaleness(
	ctx context.Context, evalCtx *eval.Context, d duration.Duration,
) (time.Time, error) {
	if d.Compare(duration.FromInt64(0)) < 0 {
		return time.Time{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"interval duration for %s must be greater or equal to 0",
			asof.WithMaxStalenessFunctionName,
		)
	}
	return duration.Add(evalCtx.GetStmtTimestamp(), d.Mul(-1)), nil
}

// EvalMinTimestamp implements the with_min_timestamp builtin.
func EvalMinTimestamp(ctx context.Context, evalCtx *eval.Context, t time.Time) (time.Time, error) {
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
