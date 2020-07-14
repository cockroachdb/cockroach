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
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
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
