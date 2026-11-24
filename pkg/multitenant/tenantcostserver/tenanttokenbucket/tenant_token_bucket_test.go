// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenanttokenbucket

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"gopkg.in/yaml.v2"
)

// TestDataDriven tests the tenant-side cost controller in an isolated setting.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		defer leaktest.AfterTest(t)()

		var ts testState
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			fn, ok := testStateCommands[d.Cmd]
			if !ok {
				d.Fatalf(t, "unknown command %s", d.Cmd)
			}
			return fn(&ts, t, d)
		})
	})
}

type testState struct {
	State
}

func (ts *testState) String() string {
	return fmt.Sprintf(
		strings.Join(
			[]string{
				"Burst Limit: %.10g",
				"Refill Rate: %.10g",
				"Current Tokens: %.10g",
				"Average Tokens: %.10g",
			}, "\n"),
		ts.TokenBurstLimit, ts.TokenRefillRate, ts.TokenCurrent, ts.TokenCurrentAvg,
	)
}

var testStateCommands = map[string]func(*testState, *testing.T, *datadriven.TestData) string{
	"reconfigure": (*testState).reconfigure,
	"update":      (*testState).update,
	"request":     (*testState).request,
}

func (ts *testState) reconfigure(t *testing.T, d *datadriven.TestData) string {
	var vals struct {
		Limit   float64
		Rate    float64
		Current float64
	}
	if err := yaml.UnmarshalStrict([]byte(d.Input), &vals); err != nil {
		d.Fatalf(t, "failed to unmarshal reconfigure values: %v", err)
	}
	ts.State.Reconfigure(
		context.Background(), roachpb.TenantID{}, vals.Current, vals.Rate, vals.Limit)
	return ts.String()
}

func parseDuration(t *testing.T, d *datadriven.TestData, str string) time.Duration {
	t.Helper()
	duration, err := time.ParseDuration(str)
	if err != nil {
		d.Fatalf(t, "failed to parse duration: %v", err)
	}
	return duration
}

func (ts *testState) update(t *testing.T, d *datadriven.TestData) string {
	ts.State.Update(parseDuration(t, d, d.Input))
	return ts.String()
}

func (ts *testState) request(t *testing.T, d *datadriven.TestData) string {
	var vals struct {
		Tokens float64
		Period string
	}
	vals.Period = "10s"
	if err := yaml.UnmarshalStrict([]byte(d.Input), &vals); err != nil {
		d.Fatalf(t, "failed to unmarshal init values: %v", err)
	}
	req := kvpb.TokenBucketRequest{
		RequestedTokens:     vals.Tokens,
		TargetRequestPeriod: parseDuration(t, d, vals.Period),
	}
	resp := ts.State.Request(context.Background(), &req)
	return fmt.Sprintf(
		strings.Join(
			[]string{
				"Granted: %.10g tokens",
				"Trickle duration: %s",
				"Fallback rate: %.10g tokens/s",
				"%s",
			},
			"\n",
		),
		resp.GrantedTokens,
		resp.TrickleDuration,
		resp.FallbackRate,
		ts.String(),
	)
}
