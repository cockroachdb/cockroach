/* Package gossip implements a protocol for sharing information
between Cockroach nodes using an ad-hoc, peer-to-peer network. The
self-assembled network aims to minimize time for new information
to reach each node, and minimize network traffic required.

Gossipped information is identified by key. Gossip information
is captured by Info objects.

Groups are used to logically group related gossip values and maintain
limits on total set size. Groups organize Info objects by key
prefix. An example is load and capacity characteristics for nodes. In
a cluster with thousands of nodes, groups force the gossip network to
limit itself to only a portion of total data volume.

A map of Info objects and a map of Group objects are kept by an
InfoStore. New Info objects should be created via InfoStore.NewInfo.
Groups are registered via: InfoStore.RegisterGroup. Info objects are
added to an InfoStore using InfoStore.AddInfo.

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

package gossip

import (
	"fmt"
)

func main() {
	fmt.Println("gossip!")
}
