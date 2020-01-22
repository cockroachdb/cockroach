// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package connect provides a no-dependency mechanism for a new or restarting
// node to discover the cluster information it needs to boot.
package connect

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
)

// Connect retrieves information that is necessary to boot a node joining an
// existing cluster. This information comes via a low-dependency rpc to an
// existing node in the cluster.
func Connect(ctx context.Context, joinList base.JoinListType) (*ConnectResponse, error) {
	panic(`WIP`)
	// TODO(dan): This is using the joinList to keep the dependencies minimal, but
	// gossip is doing all sorts of smarts with this before actually using it
	// (filtering out self, etc). On one hand it'd be nice to keep this from
	// depending on gossip (so the response could be _used_ by gossip), but it
	// would be unfortuante to duplicate this initial work.
}
