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

package gossip

import (
	"net"
)

// GossipRequest is passed to the Gossip.Gossip RPC.
type GossipRequest struct {
	Addr   net.Addr // Address of requesting node's server
	LAddr  net.Addr // Local address of client on requesting node
	MaxSeq int64    // Maximum sequence number of gossip from this peer

	Delta *infoStore // Reciprocal delta of new info since last gossip
}

// GossipResponse is returned from the Gossip.Gossip RPC.
// Delta will be nil in the event that Alternate is set.
type GossipResponse struct {
	Delta     *infoStore // Requested delta of server's infostore
	Alternate net.Addr   // Non-nil means client should retry with this address
}
