// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
)

func TestDefaultRaftConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var cfg base.RaftConfig
	cfg.SetDefaults()

	// Assert the config and various derived values.
	leaseActive, leaseRenewal := cfg.RangeLeaseDurations()
	nodeActive, nodeRenewal := cfg.NodeLivenessDurations()
	raftElectionTimeout := cfg.RaftElectionTimeout()
	raftHeartbeatInterval := cfg.RaftTickInterval * time.Duration(cfg.RaftHeartbeatIntervalTicks)

	{
		var s string
		s += spew.Sdump(cfg)
		s += fmt.Sprintf("RaftHeartbeatInterval: %s\n", raftHeartbeatInterval)
		s += fmt.Sprintf("RaftElectionTimeout: %s\n", raftElectionTimeout)
		s += fmt.Sprintf("RangeLeaseDurations: active=%s renewal=%s\n", leaseActive, leaseRenewal)
		s += fmt.Sprintf("RangeLeaseAcquireTimeout: %s\n", cfg.RangeLeaseAcquireTimeout())
		s += fmt.Sprintf("NodeLivenessDurations: active=%s renewal=%s\n", nodeActive, nodeRenewal)
		s += fmt.Sprintf("SentinelGossipTTL: %s\n", cfg.SentinelGossipTTL())
		echotest.Require(t, s, datapathutils.TestDataPath(t, "raft_config"))
	}

	// Generate and assert the derived recovery intervals.
	const (
		minRTT                = 10 * time.Millisecond
		maxRTT                = 400 * time.Millisecond // max GCP inter-region RTT is ~350ms
		maxElectionMultiplier = 2
	)

	type interval struct {
		name     string
		min, max time.Duration
	}

	formatIntervals := func(name string, intervals []interval) string {
		// Format intervals and append min/max sum.
		var minSum, maxSum time.Duration
		var formatted []interval
		for _, ival := range intervals {
			ival.name = "- " + ival.name
			formatted = append(formatted, ival)
			minSum += ival.min
			maxSum += ival.max
		}
		formatted = append(formatted, interval{name: "Total latency", min: minSum, max: maxSum})

		s := "// " + name + ":\n"
		for _, ival := range formatted {
			s += fmt.Sprintf("// %-46s [%5.2fs -%5.2fs]\n",
				ival.name, ival.min.Seconds(), ival.max.Seconds())
		}
		return s
	}

	var s string
	s += formatIntervals("Raft election", []interval{
		{
			"Heartbeat offset (0-1 heartbeat interval)",
			-raftHeartbeatInterval,
			0,
		},
		{
			fmt.Sprintf("Election timeout (random 1x-%dx timeout)", maxElectionMultiplier),
			raftElectionTimeout,
			maxElectionMultiplier * raftElectionTimeout,
		},
		{
			"Election (3x RTT: prevote, vote, append)",
			3 * minRTT,
			3 * maxRTT,
		},
	})
	s += "//\n"
	s += formatIntervals("Lease acquisition", []interval{
		{
			"Heartbeat offset (0-1 heartbeat interval)",
			-leaseRenewal,
			0,
		},
		{
			"Lease expiration (constant)",
			leaseActive,
			leaseActive,
		},
		{
			"Liveness epoch bump (2x RTT: CPut + append)",
			2 * minRTT,
			2 * maxRTT,
		},
		{
			"Lease acquisition (1x RTT: append)",
			minRTT,
			maxRTT,
		},
	})

	echotest.Require(t, s, datapathutils.TestDataPath(t, "raft_config_recovery"))
}

func TestRaftMaxInflightBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for i, tc := range []struct {
		msgSize uint64
		maxMsgs int
		maxInfl uint64
		want    uint64
	}{
		// If any of these tests fail, sync the corresponding default values with
		// config.go, and update the comments that reason about default values.
		{want: 256 << 20},                    // assert 255 MB is still default
		{maxMsgs: 128, want: 256 << 20},      // assert 128 is still default
		{msgSize: 32 << 10, want: 256 << 20}, // assert 32 KB is still default

		{maxMsgs: 1 << 30, want: 1 << 45}, // large maxMsgs
		{msgSize: 1 << 50, want: 1 << 57}, // large msgSize

		{msgSize: 100, maxMsgs: 10, maxInfl: 1000000, want: 1000000}, // reasonable
		{msgSize: 100, maxMsgs: 10, maxInfl: 5, want: 1000},          // fixup applied
		{msgSize: 1 << 50, maxMsgs: 1 << 20, want: math.MaxUint64},   // overflow
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			cfg := base.RaftConfig{
				RaftMaxInflightMsgs:  tc.maxMsgs,
				RaftMaxSizePerMsg:    tc.msgSize,
				RaftMaxInflightBytes: tc.maxInfl,
			}
			cfg.SetDefaults()
			require.Equal(t, tc.want, cfg.RaftMaxInflightBytes)
		})
	}
}
