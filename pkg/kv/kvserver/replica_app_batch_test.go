// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestShouldUseLooselyCoupledTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tests := []struct {
		settingEnabled         bool
		enginesSeparated       bool
		raftExpectedFirstIndex kvpb.RaftIndex
		wantLoose              bool
	}{
		{settingEnabled: false, enginesSeparated: false, raftExpectedFirstIndex: 0, wantLoose: false},
		{settingEnabled: false, enginesSeparated: false, raftExpectedFirstIndex: 5, wantLoose: false},
		{settingEnabled: false, enginesSeparated: true, raftExpectedFirstIndex: 0, wantLoose: true},
		{settingEnabled: false, enginesSeparated: true, raftExpectedFirstIndex: 5, wantLoose: true},
		{settingEnabled: true, enginesSeparated: false, raftExpectedFirstIndex: 0, wantLoose: false},
		{settingEnabled: true, enginesSeparated: false, raftExpectedFirstIndex: 5, wantLoose: true},
		{settingEnabled: true, enginesSeparated: true, raftExpectedFirstIndex: 0, wantLoose: true},
		{settingEnabled: true, enginesSeparated: true, raftExpectedFirstIndex: 5, wantLoose: true},
	}

	for _, tc := range tests {
		t.Run("", func(t *testing.T) {
			looselyCoupledTruncationEnabled.Override(ctx, &st.SV, tc.settingEnabled)
			require.Equal(t, tc.wantLoose, shouldUseLooselyCoupledTruncation(
				&st.SV, tc.raftExpectedFirstIndex, tc.enginesSeparated,
			))
		})
	}
}
