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

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/stretchr/testify/require"
)

func TestCauses(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	p := &causes{st: st}
	LatencyThreshold.Override(ctx, &st.SV, 100*time.Millisecond)
	HighRetryCountThreshold.Override(ctx, &st.SV, 10)

	var latencyThreshold = LatencyThreshold.Get(&st.SV)

	testCases := []struct {
		name      string
		statement *Statement
		causes    []Cause
	}{
		{
			name:      "unset",
			statement: &Statement{},
			causes:    nil,
		},
		{
			name:      "suboptimal plan",
			statement: &Statement{IndexRecommendations: []string{"THIS IS AN INDEX RECOMMENDATION"}},
			causes:    []Cause{Cause_SuboptimalPlan},
		},
		{
			name:      "high contention time",
			statement: &Statement{Contention: &latencyThreshold},
			causes:    []Cause{Cause_HighContentionTime},
		},
		{
			name:      "high retry count",
			statement: &Statement{Retries: 10},
			causes:    []Cause{Cause_HighRetryCount},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.ElementsMatch(t, tc.causes, p.examine(tc.statement))
		})
	}
}
