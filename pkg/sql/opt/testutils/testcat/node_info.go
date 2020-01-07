// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcat

import (
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// NodeInfo is the JSON schema for defining information about a node.
type NodeInfo struct {
	NodeID   int32    `json:"node_id"`
	Locality string   `json:"locality,omitempty"`
	Attrs    []string `json:"attrs,omitempty"`
}

// SetNodeInfo sets node info in the Catalog. The input to this command should
// be in the form of a JSON array, where each element of the array specifies
// info about a single node, and matches the schema defined in NodeInfo.
func (tc *Catalog) SetNodeInfo(input string) (string, error) {
	var nodeInfos []NodeInfo
	if err := json.Unmarshal([]byte(input), &nodeInfos); err != nil {
		return "", err
	}

	tc.nodes = make([]Node, len(nodeInfos))
	for i, info := range nodeInfos {
		nodeID := roachpb.NodeID(info.NodeID)
		var locality roachpb.Locality
		if err := locality.Set(info.Locality); err != nil {
			return "", err
		}
		var attrs roachpb.Attributes
		attrs.Attrs = info.Attrs
		tc.nodes[i] = Node{id: nodeID, locality: locality, attrs: attrs}
	}

	return "", nil
}
