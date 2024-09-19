// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowcontrol

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
)

// TestingKnobs provide fine-grained control over the various kvflowcontrol
// components for testing.
type TestingKnobs struct {
	V1                      TestingKnobsV1
	UseOnlyForScratchRanges bool
	// OverrideTokenDeduction is used to override how many tokens are deducted
	// post-evaluation.
	OverrideTokenDeduction func(tokens Tokens) Tokens
	// OverrideV2EnabledWhenLeaderLevel is used to override the level at which
	// RACv2 is enabled when a replica is the leader.
	OverrideV2EnabledWhenLeaderLevel func() V2EnabledWhenLeaderLevel
}

// TestingKnobsV1 are the testing knobs that appply to replication flow control
// v1, which is mostly contained in the kvflowcontroller, kvflowdispatch,
// kvflowhandle and kvflowtokentracker packages.
type TestingKnobsV1 struct {
	// UntrackTokensInterceptor is invoked whenever tokens are untracked, along
	// with their corresponding log positions.
	UntrackTokensInterceptor func(Tokens, kvflowcontrolpb.RaftLogPosition)
	// MaintainStreamsForBehindFollowers is used in tests to maintain
	// replication streams for behind followers.
	MaintainStreamsForBehindFollowers func() bool
	// MaintainStreamsForInactiveFollowers is used in tests to maintain
	// replication streams for inactive followers.
	MaintainStreamsForInactiveFollowers func() bool
	// MaintainStreamsForBrokenRaftTransport is used in tests to maintain
	// replication streams for followers we're no longer connected to via the
	// RaftTransport.
	MaintainStreamsForBrokenRaftTransport func() bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
