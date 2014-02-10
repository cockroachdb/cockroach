/*
Package gossip implements a protocol for sharing information between
Cockroach nodes. The protocol is peer to peer. It self-assembles a
network aiming to minimize time for new information to reach each
node, and minimize network traffic required.

Gossipped information is identified by key.

Groups are used to logically group related gossip values and maintain
limits on total set size. An example is load and capacity
characteristics for nodes. In a cluster with thousands of nodes,
groups force the gossip network to limit itself to only a portion of
total data volume.

Nodes are chosen as peers with intention to maximize the freshness of
gossipped information. Each node maintains a bloom filter encompassing
the keys for information the node either originated itself or learned
about from other peers in fewer than a configurable number of hops.

When selecting peers to communicate with, nodes compare bloom
filters. The bloom filters are implemented as "counting" bloom
filters, meaning instead of just a single bit at each slot in the
filter, more bits (4 or 8) are employed to keep count of how many
times the slot was incremented on key insertion. Counting bloom
filters also allow keys to be removed. An estimate of the difference
between key sets can be determined by "subtracting" one filter from
another.
*/

package main

import "fmt"

type Value interface{}

type Group struct {
	name  string // Group name
	limit int32  // Maximum number of keys in group
}

type Info struct {
	key   string  // Info key
	value Value   // Info value
	group *Group  // Group name
	node  string  // Originating node name
	key   string  // Originating range key
	ts    float64 // Wall time of origination
	ttl   float32 // Time in seconds before info is discarded
	hops  uint32  // Number of hops from originator
}

func main() {
	fmt.Println("hello world.")
}
