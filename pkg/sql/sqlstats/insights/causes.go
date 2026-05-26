// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insightspb"
)

type causes struct {
	st *cluster.Settings
}

// examine will append all causes of the statement's problems to buf and
// return the result. Buf allows the slice to be pooled.
func (c *causes) examine(
	buf []insightspb.Cause, stmt *insightspb.Statement,
) (result []insightspb.Cause) {
	result = buf
	if len(stmt.IndexRecommendations) > 0 {
		result = append(result, insightspb.Cause_SuboptimalPlan)
	}

	if stmt.Contention != nil && *stmt.Contention >= LatencyThreshold.Get(&c.st.SV) {
		result = append(result, insightspb.Cause_HighContention)
	}

	if stmt.Retries >= HighRetryCountThreshold.Get(&c.st.SV) {
		result = append(result, insightspb.Cause_HighRetryCount)
	}

	return result
}
