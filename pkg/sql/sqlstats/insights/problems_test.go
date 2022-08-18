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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/stretchr/testify/require"
)

func TestProblems(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	p := &problems{st: st}
	HighRetryCountThreshold.Override(ctx, &st.SV, 10)

	testCases := []struct {
		name      string
		statement *Statement
		problems  []Problem
	}{
		{
			name:      "unknown",
			statement: &Statement{},
			problems:  []Problem{Problem_Unknown},
		},
		{
			name:      "high retry count",
			statement: &Statement{Retries: 10},
			problems:  []Problem{Problem_HighRetryCount},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.ElementsMatch(t, tc.problems, p.examine(tc.statement))
		})
	}
}
