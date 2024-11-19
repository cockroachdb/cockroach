// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAlertingRule tests creation flow of an AlertingRule.
func TestAlertingRule(t *testing.T) {
	const alertName = "test_alert"
	const holdDuration = time.Minute
	const help = "Test alert"
	const isKV = false
	rule, err := NewAlertingRule(alertName, "invalid(sys_uptime) > 0 and resets(sys_uptime) < 5", nil, nil, holdDuration, help, isKV)
	require.NotNil(t, err)
	require.Nil(t, rule)
	_, err = NewAlertingRule(alertName, "resets(sys_uptime[10m]) > 0 and resets(sys_uptime[10m]) < 5", nil, nil, holdDuration, help, isKV)
	require.Nil(t, err)
}

// TestAggregationRule tests creation flow of an AggregationRule.
func TestAggregationRule(t *testing.T) {
	const testRule = "test_rule"
	const help = "Test rule"
	const isKV = false
	rule, err := NewAggregationRule(testRule, " invalid without(store) (capacity_available)", nil, help, isKV)
	require.NotNil(t, err)
	require.Nil(t, rule)
	_, err = NewAggregationRule(testRule, " sum without(store) (capacity_available)", nil, help, isKV)
	require.Nil(t, err)
}

func TestRuleRegistry(t *testing.T) {
	const testRule = "test_rule"
	const help = "Test rule"
	const isKV = false
	rule, err := NewAggregationRule(testRule, " sum without(store) (capacity_available)", nil, help, isKV)
	require.Nil(t, err)
	registry := NewRuleRegistry()
	registry.AddRule(rule)
	require.NotNil(t, registry.GetRuleForTest(testRule))
	require.Nil(t, registry.GetRuleForTest("invalid"))
}
