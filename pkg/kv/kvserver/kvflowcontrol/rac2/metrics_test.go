// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestBlockedStreamLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := log.ScopeWithoutShowLogs(t)
	// Causes every call to retrieve the elastic metric to log.
	prevVModule := log.GetVModule()
	_ = log.SetVModule("metrics=2")
	defer func() { _ = log.SetVModule(prevVModule) }()
	defer s.Close(t)

	ctx := context.Background()
	testStartTs := timeutil.Now()

	makeStream := func(id uint64) kvflowcontrol.Stream {
		return kvflowcontrol.Stream{
			TenantID: roachpb.MustMakeTenantID(id),
			StoreID:  roachpb.StoreID(id),
		}
	}

	st := cluster.MakeTestingClusterSettings()
	const numTokens = 1 << 20 /* 1 MiB */
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &st.SV, numTokens)
	kvflowcontrol.RegularTokensPerStream.Override(ctx, &st.SV, numTokens)
	clock := hlc.NewClockForTesting(nil)
	p := NewStreamTokenCounterProvider(st, hlc.NewClockForTesting(nil))
	metrics := NewMetrics()
	metrics.Init(p, clock)

	numBlocked := 0
	createStreamAndExhaustTokens := func(id uint64, checkMetric bool) {
		p.Eval(makeStream(id)).Deduct(ctx, admissionpb.RegularWorkClass, kvflowcontrol.Tokens(numTokens))
		if checkMetric {
			// This first call will also log.
			require.Equal(t, int64(numBlocked+1), metrics.EvalFlowControlMetrics.BlockedStreamCount[elastic].Value())
			require.Equal(t, int64(numBlocked+1), metrics.EvalFlowControlMetrics.BlockedStreamCount[regular].Value())
		}
		numBlocked++
	}
	// 1 stream that is blocked.
	id := uint64(1)
	createStreamAndExhaustTokens(id, true)
	// Total 24 streams are blocked.
	for id++; id < 25; id++ {
		createStreamAndExhaustTokens(id, false)
	}
	// 25th stream will also be blocked. The detailed stats will only cover an
	// arbitrary subset of 20 streams.
	log.Infof(ctx, "creating stream id %d", id)
	createStreamAndExhaustTokens(id, true)

	// Total 104 streams are blocked.
	for id++; id < 105; id++ {
		createStreamAndExhaustTokens(id, false)
	}
	// 105th stream will also be blocked. The blocked stream names will only
	// list 100 streams.
	log.Infof(ctx, "creating stream id %d", id)
	createStreamAndExhaustTokens(id, true)

	log.FlushFiles()
	entries, err := log.FetchEntriesFromFiles(testStartTs.UnixNano(),
		math.MaxInt64, 2000,
		regexp.MustCompile(`metrics\.go|metrics_test\.go`),
		log.WithMarkedSensitiveData)
	require.NoError(t, err)
	blockedStreamRegexp, err := regexp.Compile(
		"eval stream .* was blocked: durations: regular .* elastic .* tokens delta: regular .* elastic .*")
	require.NoError(t, err)
	blockedStreamSkippedRegexp, err := regexp.Compile(
		"skipped logging some streams that were blocked")
	require.NoError(t, err)

	const blockedCountElasticRegexp = "%d blocked eval elastic replication stream.*"
	const blockedCountRegularRegexp = "%d blocked eval regular replication stream.*"
	blocked1ElasticRegexp, err := regexp.Compile(fmt.Sprintf(blockedCountElasticRegexp, 1))
	require.NoError(t, err)
	blocked1RegularRegexp, err := regexp.Compile(fmt.Sprintf(blockedCountRegularRegexp, 1))
	require.NoError(t, err)
	blocked25ElasticRegexp, err := regexp.Compile(fmt.Sprintf(blockedCountElasticRegexp, 25))
	require.NoError(t, err)
	blocked25RegularRegexp, err := regexp.Compile(fmt.Sprintf(blockedCountRegularRegexp, 25))
	require.NoError(t, err)
	blocked105ElasticRegexp, err := regexp.Compile(
		"105 blocked eval elastic replication stream.* omitted some due to overflow")
	require.NoError(t, err)
	blocked105RegularRegexp, err := regexp.Compile(
		"105 blocked eval regular replication stream.* omitted some due to overflow")
	require.NoError(t, err)

	const creatingRegexp = "creating stream id %d"
	creating25Regexp, err := regexp.Compile(fmt.Sprintf(creatingRegexp, 25))
	require.NoError(t, err)
	creating105Regexp, err := regexp.Compile(fmt.Sprintf(creatingRegexp, 105))
	require.NoError(t, err)

	blockedStreamCount := 0
	foundBlockedElastic := false
	foundBlockedRegular := false
	foundBlockedStreamSkipped := false
	// First section of the log where 1 stream blocked. Entries are in reverse
	// chronological order.
	index := len(entries) - 1
	for ; index >= 0; index-- {
		entry := entries[index]
		if creating25Regexp.MatchString(entry.Message) {
			break
		}
		if blockedStreamRegexp.MatchString(entry.Message) {
			blockedStreamCount++
		}
		if blocked1ElasticRegexp.MatchString(entry.Message) {
			foundBlockedElastic = true
		}
		if blocked1RegularRegexp.MatchString(entry.Message) {
			foundBlockedRegular = true
		}
		if blockedStreamSkippedRegexp.MatchString(entry.Message) {
			foundBlockedStreamSkipped = true
		}
	}
	require.Equal(t, 1, blockedStreamCount)
	require.True(t, foundBlockedElastic)
	require.True(t, foundBlockedRegular)
	require.False(t, foundBlockedStreamSkipped)

	blockedStreamCount = 0
	foundBlockedElastic = false
	foundBlockedRegular = false
	// Second section of the log where 25 streams blocked.
	for ; index >= 0; index-- {
		entry := entries[index]
		if creating105Regexp.MatchString(entry.Message) {
			break
		}
		if blockedStreamRegexp.MatchString(entry.Message) {
			blockedStreamCount++
		}
		if blocked25ElasticRegexp.MatchString(entry.Message) {
			foundBlockedElastic = true
		}
		if blocked25RegularRegexp.MatchString(entry.Message) {
			foundBlockedRegular = true
		}
		if blockedStreamSkippedRegexp.MatchString(entry.Message) {
			foundBlockedStreamSkipped = true
		}
	}
	require.Equal(t, 20, blockedStreamCount)
	require.True(t, foundBlockedElastic)
	require.True(t, foundBlockedRegular)
	require.True(t, foundBlockedStreamSkipped)

	blockedStreamCount = 0
	foundBlockedElastic = false
	foundBlockedRegular = false
	// Third section of the log where 105 streams blocked.
	for ; index >= 0; index-- {
		entry := entries[index]
		if blockedStreamRegexp.MatchString(entry.Message) {
			blockedStreamCount++
		}
		if blocked105ElasticRegexp.MatchString(entry.Message) {
			foundBlockedElastic = true
		}
		if blocked105RegularRegexp.MatchString(entry.Message) {
			foundBlockedRegular = true
		}
		if blockedStreamSkippedRegexp.MatchString(entry.Message) {
			foundBlockedStreamSkipped = true
		}
	}
	require.Equal(t, 20, blockedStreamCount)
	require.True(t, foundBlockedElastic, "unable to find %v", blocked105ElasticRegexp)
	require.True(t, foundBlockedRegular, "unable to find %v", blocked105RegularRegexp)
	require.True(t, foundBlockedStreamSkipped, "unable to find %v", blockedStreamSkippedRegexp)
}
