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

/*
Package simulation provides tools meant to visualize or test aspects
of a Cockroach cluster on a single host.

Gossip

Gossip creates a gossip network of up to 250 nodes and outputs
successive visualization of the gossip network graph via dot.

Uses unix domain or tcp sockets for connecting 3, 10, 25, 50, 100 or
250 nodes. Generates .dot graph output files for each cycle of the
simulation.

To run:

    go run simulation/gossip.go -size=(small|medium|large|huge|ginormous)

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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// minDotFontSize is the minimum font size for scaling node sizes
	// proportional to the number of incoming connections.
	minDotFontSize = 12
	// maxDotFontSize is the maximum font size for scaling node sizes.
	maxDotFontSize = 24
)

var (
	size    = flag.String("size", "medium", "size of network (tiny|small|medium|large|huge|ginormous)")
	network = flag.String("network", "unix", "test with network type (unix|tcp)")
	// simGossipInterval is the compressed timescale upon which
	// simulations run.
	simGossipInterval = time.Millisecond * 150
)

// edge is a helper struct which describes an edge in the dot output graph.
type edge struct {
	dest    string // Address of destination
	added   bool   // True if edge was recently added
	deleted bool   // True if edge was recently deleted
}

// edgeMap is a map from node address to a list of edges. A helper
// method is provided to simplify adding edges.
type edgeMap map[string][]edge

// addEdge creates a list of edges if one doesn't yet exist for the
// specified node address.
func (em edgeMap) addEdge(addr string, e edge) {
	if _, ok := em[addr]; !ok {
		em[addr] = make([]edge, 0, 1)
	}
	em[addr] = append(em[addr], e)
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
func outputDotFile(dotFN string, cycle int, nodes map[string]*gossip.Gossip, edgeSet map[string]edge) string {
	f, err := os.Create(dotFN)
	if err != nil {
		log.Fatalf("unable to create temp file: %s", err)
	}
	defer f.Close()

	// Determine maximum number of incoming connections. Create outgoing
	// edges, keeping track of which are new since last time (added=true).
	outgoingMap := make(edgeMap)
	sortedAddresses := util.MapKeys(nodes).([]string)
	sort.Strings(sortedAddresses)
	var maxIncoming int
	// The order the graph file is written influences the arrangement
	// of nodes in the output image, so it makes sense to eliminate
	// randomness here. Unfortunately with graphviz it's fairly hard
	// to get a consistent ordering.
	for _, addr := range sortedAddresses {
		node := nodes[addr]
		incoming := node.Incoming()
		for _, iAddr := range incoming {
			e := edge{dest: addr}
			key := fmt.Sprintf("%s:%s", iAddr.String(), addr)
			if _, ok := edgeSet[key]; !ok {
				e.added = true
			}
			delete(edgeSet, key)
			outgoingMap.addEdge(iAddr.String(), e)
		}
		if len(incoming) > maxIncoming {
			maxIncoming = len(incoming)
		}
	}

	// Find all edges which were deleted.
	for key, e := range edgeSet {
		e.added = false
		e.deleted = true
		outgoingMap.addEdge(strings.Split(key, ":")[0], e)
		delete(edgeSet, key)
	}

	f.WriteString("digraph G {\n")
	f.WriteString("node [shape=record];\n")
	for _, addr := range sortedAddresses {
		node := nodes[addr]
		var incomplete int
		var totalAge int64
		for infoKey := range nodes {
			if infoKey == addr {
				continue // skip the node's own info
			}
			if val, err := node.GetInfo(infoKey); err != nil {
				log.Infof("error getting info for key %q: %s", infoKey, err)
				incomplete++
			} else {
				totalAge += int64(cycle) - val.(int64)
			}
		}

		var sentinelAge int64
		if val, err := node.GetInfo(gossip.KeySentinel); err != nil {
			log.Infof("error getting info for sentinel gossip key %q: %s", gossip.KeySentinel, err)
		} else {
			sentinelAge = int64(cycle) - val.(int64)
		}

		var age, nodeColor string
		if incomplete > 0 {
			nodeColor = "color=red,"
			age = fmt.Sprintf("missing %d", incomplete)
		} else {
			age = strconv.FormatFloat(float64(totalAge)/float64(len(nodes)-1), 'f', 2, 64)
		}
		fontSize := minDotFontSize
		if maxIncoming > 0 {
			fontSize = minDotFontSize + int(math.Floor(float64(len(node.Incoming())*
				(maxDotFontSize-minDotFontSize))/float64(maxIncoming)))
		}
		f.WriteString(fmt.Sprintf("\t%s [%sfontsize=%d,label=\"{%s|MH=%d, AA=%s, SA=%d}\"]\n",
			node.Name, nodeColor, fontSize, node.Name, node.MaxHops(), age, sentinelAge))
		outgoing := outgoingMap[addr]
		for _, e := range outgoing {
			dest := nodes[e.dest]
			style := ""
			if e.added {
				style = " [color=green]"
			} else if e.deleted {
				style = " [color=red,style=dotted]"
			}
			f.WriteString(fmt.Sprintf("\t%s -> %s%s\n", node.Name, dest.Name, style))
			if !e.deleted {
				edgeSet[fmt.Sprintf("%s:%s", addr, e.dest)] = e
			}
		}
	}
	f.WriteString("}\n")
	return f.Name()
}

func main() {
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
	gossipInterval := simGossipInterval
	numCycles := 10
	outputEvery := 1
	switch *size {
	case "tiny":
		// Use default parameters.
	case "small":
		nodeCount = 10
	case "medium":
		nodeCount = 25
	case "large":
		nodeCount = 50
		gossipInterval = time.Millisecond * 250
	case "huge":
		nodeCount = 100
		gossipInterval = time.Second
		numCycles = 20
		outputEvery = 2
	case "ginormous":
		nodeCount = 250
		gossipInterval = time.Second * 3
		numCycles = 20
		outputEvery = 2
	default:
		log.Fatalf("unknown simulation size: %s", *size)
	}

	edgeSet := make(map[string]edge)

	gossip.SimulateNetwork(nodeCount, *network, gossipInterval, func(cycle int, nodes map[string]*gossip.Gossip) bool {
		if cycle == numCycles {
			return false
		}
		// Update infos.
		for addr, node := range nodes {
			if err := node.AddInfo(addr, int64(cycle), time.Hour); err != nil {
				log.Infof("error updating infos addr: %s cycle: %v: %s", addr, cycle, err)
			}
		}
		// Output dot graph periodically.
		if (cycle+1)%outputEvery == 0 {
			dotFN := fmt.Sprintf("%s/sim-cycle-%d.dot", dirName, cycle)
			outputDotFile(dotFN, cycle, nodes, edgeSet)
		}

		return true
	})

	// Output instructions for viewing graphs.
	fmt.Printf("To view simulation graph output run (you must install graphviz):\n\nfor f in %s/*.dot ; do circo $f -Tpng -o $f.png ; echo $f.png ; done\n", dirName)
}
