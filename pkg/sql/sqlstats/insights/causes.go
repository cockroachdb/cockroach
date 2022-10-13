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

type causes struct {
	st *cluster.Settings
}

// examine will append all causes of the statement's problems to buf and
// return the result. Buf allows the slice to be pooled.
func (c *causes) examine(buf []Cause, stmt *Statement) (result []Cause) {
	result = buf
	if len(stmt.IndexRecommendations) > 0 {
		result = append(result, Cause_SuboptimalPlan)
	}

	if stmt.Contention != nil && *stmt.Contention >= LatencyThreshold.Get(&c.st.SV) {
		result = append(result, Cause_HighContention)
	}

	if stmt.Retries >= HighRetryCountThreshold.Get(&c.st.SV) {
		result = append(result, Cause_HighRetryCount)
	}

	return result
}
