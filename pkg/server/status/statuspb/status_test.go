// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package statuspb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestHealthCheckResult_SafeFormatter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	res := HealthCheckResult{
		Alerts: []HealthAlert{
			{
				StoreID:         1,
				SafeDescription: "some.metric",
				Value:           123.45,
			},
			{
				StoreID:         2,
				SafeDescription: "other.metric",
				Value:           99,
			},
		},
	}
	redact.Sprint(res)
	rs, ss := redact.Sprint(res), res.String()
	const exp = "some.metric@s1=123.45, other.metric@s2=99"
	require.EqualValues(t, exp, rs)
	require.EqualValues(t, exp, ss)
}
