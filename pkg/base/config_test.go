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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/davecgh/go-spew/spew"
)

func TestDefaultRaftConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var cfg base.RaftConfig
	cfg.SetDefaults()

	leaseActive, leaseRenewal := cfg.RangeLeaseDurations()
	nodeActive, nodeRenewal := cfg.NodeLivenessDurations()

	var s string
	s += spew.Sdump(cfg)
	s += fmt.Sprintf("RaftElectionTimeout: %s\n", cfg.RaftElectionTimeout())
	s += fmt.Sprintf("RangeLeaseDurations: active=%s renewal=%s\n", leaseActive, leaseRenewal)
	s += fmt.Sprintf("NodeLivenessDurations: active=%s renewal=%s\n", nodeActive, nodeRenewal)
	s += fmt.Sprintf("SentinelGossipTTL: %s\n", cfg.SentinelGossipTTL())

	echotest.Require(t, s, testutils.TestDataPath(t, "raft_config"))
}
