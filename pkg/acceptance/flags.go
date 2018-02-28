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
// permissions and limitations under the License.

package acceptance

import (
	"flag"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
)

func init() {
	flag.Parse()
}

var flagDuration = flag.Duration("d", 5*time.Second, "for duration-limited tests, how long to run them for")
var flagNodes = flag.Int("nodes", 4, "number of nodes")
var flagStores = flag.Int("stores", 1, "number of stores to use for each node")
var flagLogDir = flag.String("l", "", "the directory to store log files, relative to the test source")

// readConfigFromFlags will convert the flags to a TestConfig for the purposes
// of starting up a cluster.
func readConfigFromFlags() cluster.TestConfig {
	cfg := cluster.TestConfig{
		Name:     fmt.Sprintf("AdHoc %dx%d", *flagNodes, *flagStores),
		Duration: *flagDuration,
	}
	for i := 0; i < *flagNodes; i++ {
		cfg.Nodes = append(cfg.Nodes, cluster.NodeConfig{
			Stores: make([]cluster.StoreConfig, *flagStores),
		})
	}
	return cfg
}
