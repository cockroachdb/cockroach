// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insightspb"
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
		statement *insightspb.Statement
		causes    []insightspb.Cause
	}{
		{
			name:      "unset",
			statement: &insightspb.Statement{},
			causes:    nil,
		},
		{
			name:      "suboptimal plan",
			statement: &insightspb.Statement{IndexRecommendations: []string{"THIS IS AN INDEX RECOMMENDATION"}},
			causes:    []insightspb.Cause{insightspb.Cause_SuboptimalPlan},
		},
		{
			name:      "high contention time",
			statement: &insightspb.Statement{Contention: &latencyThreshold},
			causes:    []insightspb.Cause{insightspb.Cause_HighContention},
		},
		{
			name:      "high retry count",
			statement: &insightspb.Statement{Retries: 10},
			causes:    []insightspb.Cause{insightspb.Cause_HighRetryCount},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.ElementsMatch(t, tc.causes, p.examine(nil /* buf */, tc.statement))
		})
	}
}
