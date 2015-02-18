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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Shawn Morel (shawn@strangemonad.com)

// Package status defines the data types of cluster-wide and per-node status responses.
package status

// A Cluster that contains nodes.
type Cluster struct{}

// NodeList contains a slice of summaries for each Node.
type NodeList struct {
	Nodes []NodeSummary `json:"nodes"`
}

// A NodeSummary contains a summary for a particular node.
type NodeSummary struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// Node represents an individual node within the cluster.
type Node struct{}
