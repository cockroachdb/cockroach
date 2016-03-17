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

import "time"

// DefaultDuration is the default duration for each individual test.
const DefaultDuration = 5 * time.Second

// DefaultConfigs returns a list of standard tests to run against acceptance
// tests.
func DefaultConfigs() []TestConfig {
	return []TestConfig{
		{
			Name: "3x1",
			Nodes: []NodeConfig{
				{
					Count:  3,
					Stores: []StoreConfig{{Count: 1}},
				},
			},
		},
		{
			Name: "5x1",
			Nodes: []NodeConfig{
				{
					Count:  5,
					Stores: []StoreConfig{{Count: 1}},
				},
			},
		},
		{
			Name: "7x1",
			Nodes: []NodeConfig{
				{
					Count:  7,
					Stores: []StoreConfig{{Count: 1}},
				},
			},
		},
		{
			Name: "3x2",
			Nodes: []NodeConfig{
				{
					Count:  3,
					Stores: []StoreConfig{{Count: 2}},
				},
			},
		},
		{
			Name: "1x1L,1x1M,1x1S",
			Nodes: []NodeConfig{
				{
					Count: 1,
					Stores: []StoreConfig{{
						Count:     1,
						MaxRanges: 10,
					}},
				},
				{
					Count: 1,
					Stores: []StoreConfig{{
						Count:     1,
						MaxRanges: 20,
					}},
				},
				{
					Count: 1,
					Stores: []StoreConfig{{
						Count:     1,
						MaxRanges: 30,
					}},
				},
			},
		},
		{
			Name: "1x1,1x2,1x3",
			Nodes: []NodeConfig{
				{
					Count:  1,
					Stores: []StoreConfig{{Count: 1}},
				},
				{
					Count:  1,
					Stores: []StoreConfig{{Count: 2}},
				},
				{
					Count:  1,
					Stores: []StoreConfig{{Count: 3}},
				},
			},
		},
	}
}
