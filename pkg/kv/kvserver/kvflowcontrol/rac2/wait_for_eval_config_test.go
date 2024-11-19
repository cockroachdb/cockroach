// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestWaitForEvalConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	st := cluster.MakeTestingClusterSettings()
	ctx := context.Background()
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)
	kvflowcontrol.Mode.Override(ctx, &st.SV, kvflowcontrol.ApplyToAll)

	// All work waits for eval.
	w := NewWaitForEvalConfig(st)
	wec, ch1 := w.Current()
	require.Equal(t, AllWorkWaitsForEval, wec)
	require.NotNil(t, ch1)
	require.False(t, wec.Bypass(admissionpb.ElasticWorkClass))
	require.False(t, wec.Bypass(admissionpb.RegularWorkClass))

	// No work waits for eval.
	kvflowcontrol.Enabled.Override(ctx, &st.SV, false)
	var ch2 <-chan struct{}
	wec, ch2 = w.Current()
	require.Equal(t, NoWorkWaitsForEval, wec)
	require.NotNil(t, ch2)
	require.NotEqual(t, ch1, ch2)
	require.True(t, wec.Bypass(admissionpb.ElasticWorkClass))
	require.True(t, wec.Bypass(admissionpb.RegularWorkClass))

	// All work waits for eval.
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)
	var ch3 <-chan struct{}
	wec, ch3 = w.Current()
	require.Equal(t, AllWorkWaitsForEval, wec)
	// Channel has not changed.
	require.Equal(t, ch2, ch3)

	// Elastic work waits for eval.
	kvflowcontrol.Mode.Override(ctx, &st.SV, kvflowcontrol.ApplyToElastic)
	var ch4 <-chan struct{}
	wec, ch4 = w.Current()
	require.Equal(t, OnlyElasticWorkWaitsForEval, wec)
	require.NotNil(t, ch4)
	require.NotEqual(t, ch3, ch4)
	require.False(t, wec.Bypass(admissionpb.ElasticWorkClass))
	require.True(t, wec.Bypass(admissionpb.RegularWorkClass))

	// All work waits for eval.
	kvflowcontrol.Mode.Override(ctx, &st.SV, kvflowcontrol.ApplyToAll)
	var ch5 <-chan struct{}
	wec, ch5 = w.Current()
	require.Equal(t, AllWorkWaitsForEval, wec)
	// Channel has not changed.
	require.Equal(t, ch4, ch5)
}
