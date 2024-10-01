// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverrules

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

// TestMetricRules tests the creation of metric rules related
// to server metrics.
func TestMetricRules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ruleRegistry := metric.NewRuleRegistry()
	CreateAndAddRules(context.Background(), ruleRegistry)
	require.NotNil(t, ruleRegistry.GetRuleForTest(nodeRestartRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(nodeCACertExpiryRuleName))
	require.NotNil(t, ruleRegistry.GetRuleForTest(nodeCertExpiryRuleName))
	require.Equal(t, 3, ruleRegistry.GetRuleCountForTest())
}
