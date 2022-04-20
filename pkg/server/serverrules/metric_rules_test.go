// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
