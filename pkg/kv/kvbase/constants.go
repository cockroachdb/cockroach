// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvbase

// FollowerReadServingMsg is a log message that needs to be used for tests in
// other packages.
const FollowerReadServingMsg = "serving via follower read"

// RoutingRequestLocallyMsg is a log message that needs to be used for tests in
// other packages.
const RoutingRequestLocallyMsg = "sending request to local client"

// RoutingRequestToSameRegionAndZoneMsg is a log message that needs to be used for
// tests in other packages.
const RoutingRequestToSameRegionAndZoneMsg = "sending request within the same region and zone"

// SpawningHeartbeatLoopMsg is a log message that needs to be used for tests in
// other packages.
const SpawningHeartbeatLoopMsg = "coordinator spawns heartbeat loop"
