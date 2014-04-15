// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/util"
)

// An allocator makes allocation decisions based on a zone
// configuration, existing range metadata and available servers &
// disks. Configuration settings and range metadata information is
// stored directly in the engine-backed range they describe.
// Information on suitability and availability of servers is
// gleaned from the gossip network.
type allocator struct {
	gossip *gossip.Gossip
}

// allocate returns a suitable Replica for the range and zone. If none
// are available / suitable, returns an error.
//
// TODO(spencer): currently this just returns a random device from
// amongst the available servers on the gossip network; need to add
// zone config and replica metadata.
func (a *allocator) allocate() (*Replica, error) {
	// TODO(spencer): choose at random from gossip network's available Disks.
	return nil, util.Errorf("unimplemented")
}
