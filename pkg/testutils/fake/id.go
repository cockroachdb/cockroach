// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package fake

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// NodeID is a dummy node ID for use in tests. It always stores 1.
var NodeID = func() *base.NodeIDContainer {
	nodeID := base.NodeIDContainer{}
	nodeID.Reset(1)
	return &nodeID
}()

// ClusterID is a dummy cluster ID for use in tests. It always returns
// "deadbeef-dead-beef-dead-beefdeadbeef" as a uuid.UUID.
var ClusterID = func() func() uuid.UUID {
	clusterID := uuid.FromUint128(uint128.FromInts(0xdeadbeefdeadbeef, 0xdeadbeefdeadbeef))
	return func() uuid.UUID { return clusterID }
}()
