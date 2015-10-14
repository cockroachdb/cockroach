// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package main

import (
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/util/stop"
)

func main() {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	fmt.Printf("A simulation of the cluster's rebalancing.\n\n")

	// TODO(bram): Setup flags to allow filenames for these outputs. Print both
	// to action and epoch to os.Stdout as well.
	epochWriter := os.Stdout
	actionWriter := os.Stdout
	c := createCluster(stopper, 5, epochWriter, actionWriter)

	// Split a random range 100 times.
	for i := 0; i < 10; i++ {
		c.splitRangeRandom()
	}
	c.flush()

	// TODO(bram): only flush when on manual stepping (once that enabled).
	// Run until stable or at the 100th epoch.
	for c.runEpoch() != true && c.epoch < 100 {
		c.flush()
	}
	c.flush()
	fmt.Println(c)

	for i := 0; i < 70; i++ {
		c.splitRangeLast()
	}

	// Run until stable or at the 1000th epoch.
	for c.runEpoch() != true && c.epoch < 1000 {
		c.flush()
	}
	c.flush()

	fmt.Println(c)
}
