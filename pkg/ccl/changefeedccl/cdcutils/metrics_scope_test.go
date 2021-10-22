// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdcutils

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

func TestMetricScope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	scope := CreateSLIScopes()
	require.NotNil(t, scope)

	sliMetricNames := []string{
		"error_retries",
	}

	expectedMetrics := make(map[string]struct{})
	expectScopedMetrics := func(scope string) {
		for _, n := range sliMetricNames {
			expectedMetrics[fmt.Sprintf("changefeed.%s.%s", scope, n)] = struct{}{}
		}
	}

	for i := 0; i < maxSLIScopes; i++ {
		scopeName := fmt.Sprintf("tier%d", i)
		require.NotNil(t, scope.GetSLIMetrics(scopeName))
		expectScopedMetrics(scopeName)
	}

	require.Nil(t, scope.GetSLIMetrics("does not exist"))
	require.Nil(t, scope.GetSLIMetrics(fmt.Sprintf("tier%d", maxSLIScopes+1)))

	registry := metric.NewRegistry()
	registry.AddMetricStruct(scope)
	registry.Each(func(name string, _ interface{}) {
		_, exists := expectedMetrics[name]
		require.True(t, exists)
		delete(expectedMetrics, name)
	})
	require.Equal(t, 0, len(expectedMetrics),
		"remaining metrics: %s", expectedMetrics)
}
