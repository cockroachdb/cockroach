// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
