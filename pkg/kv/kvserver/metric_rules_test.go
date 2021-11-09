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
	metricRules := NewMetricRules(ruleRegistry)
	metricRules.CreateAndAddRules(context.Background())
	require.NotNil(t, metricRules.unavailableRanges)
	require.Equal(t, ruleRegistry.GetRuleForTest(metricRules.unavailableRanges.Name()), metricRules.unavailableRanges)
	require.NotNil(t, metricRules.underreplicatedRanges)
	require.Equal(t, ruleRegistry.GetRuleForTest(metricRules.underreplicatedRanges.Name()), metricRules.underreplicatedRanges)
	require.NotNil(t, metricRules.requestsStuckInRaft)
	require.Equal(t, ruleRegistry.GetRuleForTest(metricRules.requestsStuckInRaft.Name()), metricRules.requestsStuckInRaft)
	require.NotNil(t, metricRules.highOpenFDCount)
	require.Equal(t, ruleRegistry.GetRuleForTest(metricRules.highOpenFDCount.Name()), metricRules.highOpenFDCount)
	require.NotNil(t, metricRules.nodeCapacity)
	require.Equal(t, ruleRegistry.GetRuleForTest(metricRules.nodeCapacity.Name()), metricRules.nodeCapacity)
	require.NotNil(t, metricRules.clusterCapacity)
	require.Equal(t, ruleRegistry.GetRuleForTest(metricRules.clusterCapacity.Name()), metricRules.clusterCapacity)
	require.NotNil(t, metricRules.nodeCapacityAvailable)
	require.Equal(t, ruleRegistry.GetRuleForTest(metricRules.nodeCapacityAvailable.Name()), metricRules.nodeCapacityAvailable)
	require.NotNil(t, metricRules.clusterCapacityAvailable)
	require.Equal(t, ruleRegistry.GetRuleForTest(metricRules.clusterCapacityAvailable.Name()), metricRules.clusterCapacityAvailable)
	require.NotNil(t, metricRules.capacityAvailableRatio)
	require.Equal(t, ruleRegistry.GetRuleForTest(metricRules.capacityAvailableRatio.Name()), metricRules.capacityAvailableRatio)
	require.NotNil(t, metricRules.nodeCapacityAvailableRatio)
	require.Equal(t, ruleRegistry.GetRuleForTest(metricRules.nodeCapacityAvailableRatio.Name()), metricRules.nodeCapacityAvailableRatio)
	require.NotNil(t, metricRules.clusterCapacityAvailableRatio)
	require.Equal(t, ruleRegistry.GetRuleForTest(metricRules.clusterCapacityAvailableRatio.Name()), metricRules.clusterCapacityAvailableRatio)
}
