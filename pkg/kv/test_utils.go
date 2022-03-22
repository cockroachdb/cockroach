// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// OnlyFollowerReads looks through all the RPCs and asserts that every single
// one resulted in a follower read. Returns false if no RPCs are found.
func OnlyFollowerReads(rec tracing.Recording) bool {
	foundFollowerRead := false
	for _, sp := range rec {
		if sp.Operation == "/cockroach.roachpb.Internal/Batch" &&
			sp.Tags["span.kind"] == "server" {
			if tracing.LogsContainMsg(sp, kvbase.FollowerReadServingMsg) {
				foundFollowerRead = true
			} else {
				return false
			}
		}
	}
	return foundFollowerRead
}

// IsExpectedRelocateError maintains an allowlist of errors related to
// atomic-replication-changes we want to ignore / retry on for tests.
// See:
// https://github.com/cockroachdb/cockroach/issues/33708
// https://github.cm/cockroachdb/cockroach/issues/34012
// https://github.com/cockroachdb/cockroach/issues/33683#issuecomment-454889149
// for more failure modes not caught here.
//
// Note that whenever possible, callers should rely on
// kvserver.Is{Retryable,Illegal}ReplicationChangeError,
// which avoids string matching.
func IsExpectedRelocateError(err error) bool {
	allowlist := []string{
		"descriptor changed",
		"unable to remove replica .* which is not present",
		"unable to add replica .* which is already present",
		"none of the remaining voters .* are legal additions", // https://github.com/cockroachdb/cockroach/issues/74902
		"received invalid ChangeReplicasTrigger .* to remove self",
		"raft group deleted",
		"snapshot failed",
		"breaker open",
		"unable to select removal target", // https://github.com/cockroachdb/cockroach/issues/49513
		"cannot up-replicate to .*; missing gossiped StoreDescriptor",
		"remote couldn't accept .* snapshot",
		"cannot add placeholder",
		"removing leaseholder not allowed since it isn't the Raft leader",
		"could not find a better lease transfer target for",
	}
	pattern := "(" + strings.Join(allowlist, "|") + ")"
	return testutils.IsError(err, pattern)
}
