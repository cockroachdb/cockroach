// Copyright 2016 The Cockroach Authors.
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
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package cluster

import (
	"bytes"
	"fmt"
	"time"
)

const (
	// DefaultStall is the default value for stall detection time.
	DefaultStall = 2 * time.Minute
	// DefaultDuration is the default duration for each individual test.
	DefaultDuration = 5 * time.Second
)

// DefaultConfigs returns a list of standard tests to run against acceptance
// tests.
func DefaultConfigs() []TestConfig {
	return []TestConfig{
		{
			Name:     "3x1",
			Duration: DefaultDuration,
			Stall:    DefaultStall,
			Nodes: []NodeConfig{
				{
					Count:  3,
					Stores: []StoreConfig{{Count: 1}},
				},
			},
		},
		/*  TODO(bram): #4445 skipping these test cases until is resolved.
		{
			Name:     "5x1",
			Duration: DefaultDuration,
			Stall:    DefaultStall,
			Nodes: []NodeConfig{
				{
					Count:  5,
					Stores: []StoreConfig{{Count: 1}},
				},
			},
		},
		{
			Name: "7x1",
			Duration: DefaultDuration,
			Stall:    DefaultStall,
			Nodes: []NodeConfig{
				{
					Count:  7,
					Stores: []StoreConfig{{Count: 1}},
				},
			},
		},*/
	}
}

// PrettyString creates a human readable string depicting the test and cluster
// setup designed for output while running tests.
func (tc TestConfig) PrettyString() string {
	var nodeCount int
	var storeCount int
	var buffer bytes.Buffer

	buffer.WriteString(fmt.Sprintf("Cluster Setup: %s\nDuration: %s, Stall: %s\n", tc.Name, tc.Duration, tc.Stall))
	for _, nc := range tc.Nodes {
		for i := 0; i < int(nc.Count); i++ {
			buffer.WriteString(fmt.Sprintf("Node %d - ", nodeCount))
			nodeCount++
			var tmpStoreCount int
			for _, sc := range nc.Stores {
				for j := 0; j < int(sc.Count); j++ {
					if tmpStoreCount > 0 {
						buffer.WriteString(", ")
					}
					// TODO(bram): #4561 Add store details (size) here when
					// they are available.
					buffer.WriteString(fmt.Sprintf("Store %d", tmpStoreCount))
					tmpStoreCount++
					storeCount++
				}
			}
			buffer.WriteString("\n")
		}
	}
	buffer.WriteString(fmt.Sprintf("Total Nodes Count:%d, Total Store Count:%d\n", nodeCount, storeCount))

	return buffer.String()
}
