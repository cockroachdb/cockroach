// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvbase

// FollowerReadServingMsg is a log message that needs to be used for tests in
// other packages.
const FollowerReadServingMsg = "serving via follower read"

// RoutingRequestLocallyMsg is a log message that needs to be used for tests in
// other packages.
const RoutingRequestLocallyMsg = "sending request to local client"

// SpawningHeartbeatLoopMsg is a log message that needs to be used for tests in
// other packages.
const SpawningHeartbeatLoopMsg = "coordinator spawns heartbeat loop"
