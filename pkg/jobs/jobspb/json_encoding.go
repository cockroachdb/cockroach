// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobspb

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// tsOrNull converts micros in a timestamp datum if micros is positive. It
// returns null datum if micros is 0.
func tsOrNull(micros int64) (tree.Datum, error) {
	if micros == 0 {
		return tree.DNull, nil
	}
	ts := timeutil.Unix(0, micros*time.Microsecond.Nanoseconds())
	return tree.MakeDTimestamp(ts, time.Microsecond)
}

// TransitionLogToJSON returns a formatted JSON object from a TransitionLog.
// Encoded JSON consists of four properties:
// {
//    "coordinator_id" : number
//    "resume_exit_error" : string
//    "resume_start_micros" : timestamp string, formatted by tree.DTimestamp
//    "state" : string
// }
// Default values of coordinator_id is 0 whereas it is tree.DNull string for the rest.
func (tl *TransitionLog) TransitionLogToJSON(ctx context.Context) (json.JSON, error) {
	b := json.NewObjectBuilder(4)

	b.Add("coordinator_id", json.FromInt(int(tl.CoordinatorId)))

	if len(tl.State) > 0 {
		b.Add("state", json.FromString(tl.State))
	} else {
		b.Add("state", json.FromString(tree.DNull.String()))
	}

	ts, err := tsOrNull(tl.ResumeStartMicros)
	if err != nil {
		return nil, err
	}
	b.Add("resume_start_micros", json.FromString(ts.String()))

	var errVals string
	if tl.ResumeExitError.Size() > 0 {
		errVals = errors.DecodeError(ctx, *tl.ResumeExitError).Error()
	} else {
		errVals = tree.DNull.String()
	}
	b.Add("resume_exit_error", json.FromString(errVals))

	return b.Build(), nil
}
