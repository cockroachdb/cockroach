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
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

/*
Package simulation provides tools meant to visualize or test aspects
of a Cockroach cluster on a single host.

Gossip

Gossip creates a gossip network of up to 250 nodes and outputs
successive visualization of the gossip network graph via dot.

Uses tcp sockets for connecting 3, 10, 25, 50, 100 or 250
nodes. Generates .dot graph output files for each cycle of the
simulation.

To run:

    go install github.com/cockroachdb/cockroach/cmd/gossipsim
    gossipsim -size=(small|medium|large|huge|ginormous)

Log output includes instructions for displaying the graph output as a
series of images to visualize the evolution of the network.

Running the large through ginormous simulations will require the open
files limit be increased either for the shell running the simulation,
or system wide. For Linux:

    # For the current shell:
    ulimit -n 65536

    # System-wide:
    sysctl fs.file-max
    fs.file-max = 50384

For MacOS:

    # To view current limits (soft / hard):
    launchctl limit maxfiles

    # To edit, add/edit the following line in /etc/launchd.conf and
    # restart for the new file limit to take effect.
    #
    # limit maxfiles 16384 32768
    sudo vi /etc/launchd.conf
*/
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/simulation"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
)

const (
	// minDotFontSize is the minimum font size for scaling node sizes
	// proportional to the number of incoming connections.
	minDotFontSize = 12
	// maxDotFontSize is the maximum font size for scaling node sizes.
	maxDotFontSize = 24
)

var (
	size = flag.String("size", "medium", "size of network (tiny|small|medium|large|huge|ginormous)")
)

// edge is a helper struct which describes an edge in the dot output graph.
type edge struct {
	dest    roachpb.NodeID // Node ID of destination
	added   bool           // True if edge was recently added
	deleted bool           // True if edge was recently deleted
}

// edgeMap is a map from node address to a list of edges. A helper
// method is provided to simplify adding edges.
type edgeMap map[roachpb.NodeID][]edge

// addEdge creates a list of edges if one doesn't yet exist for the
// specified node ID.
func (em edgeMap) addEdge(nodeID roachpb.NodeID, e edge) {
	if _, ok := em[nodeID]; !ok {
		em[nodeID] = make([]edge, 0, 1)
	}
	em[nodeID] = append(em[nodeID], e)
}

// outputDotFile generates a .dot file describing the current state of
// the gossip network. nodes is a map from network address to gossip
// node. edgeSet is empty on the first invocation, but
// its content is set to encompass the entire set of edges in the
// network when this method returns. It should be resupplied with each
// successive invocation, as it is used to determine which edges are
// new and which have been deleted and show those changes visually in
// the output graph. New edges are drawn green; edges which were
// removed over the course of the last simulation step(s) are drawn in
// a lightly-dashed red.
//
// The format of the output looks like this:
//
//   digraph G {
//   node [shape=record];
//        node1 [fontsize=12,label="{Node 1|MH=3}"]
//        node1 -> node3 [color=green]
//        node1 -> node4
//        node1 -> node5 [color=red,style=dotted]
//        node2 [fontsize=24,label="{Node 2|MH=2}"]
//        node2 -> node5
//        node3 [fontsize=18,label="{Node 3|MH=5}"]
//        node3 -> node5
//        node3 -> node4
//        node4 [fontsize=24,label="{Node 4|MH=4}"]
//        node4 -> node2
//        node5 [fontsize=24,label="{Node 5|MH=1}"]
//        node5 -> node2
//        node5 -> node3
//   }
//
// Returns the name of the output file and a boolean for whether or not
// the network has quiesced (that is, no new edges, and all nodes are
// connected).
func outputDotFile(dotFN string, cycle int, network *simulation.Network, edgeSet map[string]edge) (string, bool) {
	f, err := os.Create(dotFN)
	if err != nil {
		log.Fatalf("unable to create temp file: %s", err)
	}
	defer f.Close()

	// Determine maximum number of incoming connections. Create outgoing
	// edges, keeping track of which are new since last time (added=true).
	outgoingMap := make(edgeMap)
	var maxIncoming int
	quiescent := true
	// The order the graph file is written influences the arrangement
	// of nodes in the output image, so it makes sense to eliminate
	// randomness here. Unfortunately with graphviz it's fairly hard
	// to get a consistent ordering.
	for _, simNode := range network.Nodes {
		node := simNode.Gossip
		incoming := node.Incoming()
		for _, iNode := range incoming {
			e := edge{dest: node.GetNodeID()}
			key := fmt.Sprintf("%d:%d", iNode, node.GetNodeID())
			if _, ok := edgeSet[key]; !ok {
				e.added = true
				quiescent = false
			}
			delete(edgeSet, key)
			outgoingMap.addEdge(iNode, e)
		}
		if len(incoming) > maxIncoming {
			maxIncoming = len(incoming)
		}
	}

	// Find all edges which were deleted.
	for key, e := range edgeSet {
		e.added = false
		e.deleted = true
		quiescent = false
		nodeID, err := strconv.Atoi(strings.Split(key, ":")[0])
		if err != nil {
			log.Fatal(err)
		}
		outgoingMap.addEdge(roachpb.NodeID(nodeID), e)
		delete(edgeSet, key)
	}

	fmt.Fprintln(f, "digraph G {")
	fmt.Fprintln(f, "node [shape=record];")
	for _, simNode := range network.Nodes {
		node := simNode.Gossip
		var missing []roachpb.NodeID
		var totalAge int64
		for _, otherNode := range network.Nodes {
			if otherNode == simNode {
				continue // skip the node's own info
			}
			infoKey := otherNode.Addr.String()
			// GetInfo returns an error if the info is missing.
			if info, err := node.GetInfo(infoKey); err != nil {
				missing = append(missing, otherNode.Gossip.GetNodeID())
				quiescent = false
			} else {
				_, val, err := encoding.DecodeUint64Ascending(info)
				if err != nil {
					log.Fatalf("bad decode of node info cycle: %s", err)
				}
				totalAge += int64(cycle) - int64(val)
			}
		}
		log.Infof("node %d: missing infos for nodes %s", node.GetNodeID(), missing)

		var sentinelAge int64
		// GetInfo returns an error if the info is missing.
		if info, err := node.GetInfo(gossip.KeySentinel); err != nil {
			log.Infof("error getting info for sentinel gossip key %q: %s", gossip.KeySentinel, err)
		} else {
			_, val, err := encoding.DecodeUint64Ascending(info)
			if err != nil {
				log.Fatalf("bad decode of sentinel cycle: %s", err)
			}
			sentinelAge = int64(cycle) - int64(val)
		}

		var age, nodeColor string
		if len(missing) > 0 {
			nodeColor = "color=red,"
			age = fmt.Sprintf("missing %d", len(missing))
		} else {
			age = strconv.FormatFloat(float64(totalAge)/float64(len(network.Nodes)-1-len(missing)), 'f', 4, 64)
		}
		fontSize := minDotFontSize
		if maxIncoming > 0 {
			fontSize = minDotFontSize + int(math.Floor(float64(len(node.Incoming())*
				(maxDotFontSize-minDotFontSize))/float64(maxIncoming)))
		}
		fmt.Fprintf(f, "\t%s [%sfontsize=%d,label=\"{%s|AA=%s, MH=%d, SA=%d}\"]\n",
			node.GetNodeID(), nodeColor, fontSize, node.GetNodeID(), age, node.MaxHops(), sentinelAge)
		outgoing := outgoingMap[node.GetNodeID()]
		for _, e := range outgoing {
			destSimNode, ok := network.GetNodeFromID(e.dest)
			if !ok {
				continue
			}
			dest := destSimNode.Gossip
			style := ""
			if e.added {
				style = " [color=green]"
			} else if e.deleted {
				style = " [color=red,style=dotted]"
			}
			fmt.Fprintf(f, "\t%s -> %s%s\n", node.GetNodeID(), dest.GetNodeID(), style)
			if !e.deleted {
				edgeSet[fmt.Sprintf("%d:%d", node.GetNodeID(), e.dest)] = e
			}
		}
	}
	fmt.Fprintln(f, "}")
	return f.Name(), quiescent
}

func main() {
	// Seed the random number generator for non-determinism across
	// multiple runs.
	randutil.SeedForTests()

	if f := flag.Lookup("alsologtostderr"); f != nil {
		fmt.Println("Starting simulation. Add -alsologtostderr to see progress.")
	}
	flag.Parse()

	dirName, err := ioutil.TempDir("", "gossip-simulation-")
	if err != nil {
		log.Fatalf("could not create temporary directory for gossip simulation output: %s", err)
	}

	// Simulation callbacks to run the simulation for cycleCount
	// cycles. At each cycle % outputEvery, a dot file showing the
	// state of the network graph is output.
	nodeCount := 3
	switch *size {
	case "tiny":
		// Use default parameters.
	case "small":
		nodeCount = 10
	case "medium":
		nodeCount = 25
	case "large":
		nodeCount = 50
	case "huge":
		nodeCount = 100
	case "ginormous":
		nodeCount = 250
	default:
		log.Fatalf("unknown simulation size: %s", *size)
	}

	edgeSet := make(map[string]edge)

	n := simulation.NewNetwork(nodeCount)
	n.SimulateNetwork(
		func(cycle int, network *simulation.Network) bool {
			// Output dot graph.
			dotFN := fmt.Sprintf("%s/sim-cycle-%03d.dot", dirName, cycle)
			_, quiescent := outputDotFile(dotFN, cycle, network, edgeSet)
			// Run until network has quiesced.
			return !quiescent
		})
	n.Stop()

	// Output instructions for viewing graphs.
	fmt.Printf("To view simulation graph output run (you must install graphviz):\n\nfor f in %s/*.dot ; do circo $f -Tpng -o $f.png ; echo $f.png ; done\n", dirName)
}
