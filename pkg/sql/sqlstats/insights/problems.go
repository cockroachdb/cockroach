// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package insights

import "github.com/cockroachdb/cockroach/pkg/settings/cluster"

type problems struct {
	st *cluster.Settings
}

func (p *problems) examine(stmt *Statement) (result []Problem) {
	if len(stmt.IndexRecommendations) > 0 {
		result = append(result, Problem_SuboptimalPlan)
	}

	if stmt.Retries >= HighRetryCountThreshold.Get(&p.st.SV) {
		result = append(result, Problem_HighRetryCount)
	}

	if stmt.Status == Statement_Failed {
		result = append(result, Problem_FailedExecution)
	}

	if len(result) == 0 {
		result = append(result, Problem_Unknown)
	}

	return
}
