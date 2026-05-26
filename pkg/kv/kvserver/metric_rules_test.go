// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

// TestMetricRules tests the creation of metric rules related
// to KV metrics.
func TestMetricRules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ruleRegistry := metric.NewRuleRegistry()
	CreateAndAddRules(context.Background(), ruleRegistry)
	require.NotNil(t, ruleRegistry.GetRuleForTest(unavailableRangesRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(trippedReplicaCircuitBreakersRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(underreplicatedRangesRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(requestsStuckInRaftRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(highOpenFDCountRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(nodeCapacityRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(clusterCapacityRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(nodeCapacityAvailableRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(clusterCapacityAvailableRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(capacityAvailableRatioRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(nodeCapacityAvailableRatioRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(clusterCapacityAvailableRatioRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(nodeCapacityLowRuleName))
	require.Equal(t, 13, ruleRegistry.GetRuleCountForTest())
}
