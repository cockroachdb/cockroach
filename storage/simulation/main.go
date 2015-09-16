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

	"github.com/cockroachdb/cockroach/util/stop"
)

func main() {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	c := createCluster(stopper, 5)

	fmt.Printf("A simulation of the cluster's rebalancing.\n\n")
	fmt.Printf("Cluster Info:\n%s\n", c)
}
