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

// TransitionLogToJSON returns a formatted JSON object from a TransitionLog.
// Encoded JSON consists of four properties:
// {
//    "coordinator_id" : number
//    "resume_exit_error" : string
//    "resume_start_micros" : timestamp string, formatted by tree.DTimestamp
//    "state" : string
// }
func (tl *TransitionLog) TransitionLogToJSON(ctx context.Context) (json.JSON, error) {
	b := json.NewObjectBuilder(4)

	// Node IDs start with 1.
	if tl.CoordinatorId > 0 {
		b.Add("coordinator_id", json.FromInt(int(tl.CoordinatorId)))
	}

	if len(tl.State) > 0 {
		b.Add("state", json.FromString(tl.State))
	}

	if tl.ResumeStartMicros > 0 {
		uts := timeutil.Unix(0, tl.ResumeStartMicros*time.Microsecond.Nanoseconds())
		ts, err := tree.MakeDTimestamp(uts, time.Microsecond)
		if err != nil {
			return nil, err
		}
		b.Add("resume_start_micros", json.FromString(ts.String()))
	}

	if tl.ResumeExitError.Size() > 0 {
		errVals := errors.DecodeError(ctx, *tl.ResumeExitError).Error()
		b.Add("resume_exit_error", json.FromString(errVals))
	}

	return b.Build(), nil
}
