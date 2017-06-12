// Copyright 2017 The Cockroach Authors.
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

package server

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
)

const (
	debugClassCellLeftHeader = "left-header"
	debugClassCellShort      = "short"
	debugClassWarning        = "warning"
)

// This specifies the order in which these values are displayed.
const (
	debugNodesHeaderNode = iota
	debugNodesHeaderAddress
	debugNodesHeaderIP
	debugNodesHeaderLocality
	debugNodesHeaderAttributes
	debugNodesHeaderEnvironment
	debugNodesHeaderArguments
	debugNodesHeaderTag
	debugNodesHeaderRevision
	debugNodesHeaderTime
	debugNodesHeaderType
	debugNodesHeaderPlatform
	debugNodesHeaderGoVersion
	debugNodesHeaderCGO
	debugNodesHeaderDistribution
	debugNodesHeaderStartedAt
	debugNodesHeaderUpdatedAt
)

var debugNodesHeaders = []struct {
	title string // This is displayed on the page.
	check bool   // Show a warning if all values are not equal.
}{
	debugNodesHeaderNode:         {"Node", false},
	debugNodesHeaderAddress:      {"Address", false},
	debugNodesHeaderIP:           {"IP", false},
	debugNodesHeaderLocality:     {"Locality", false},
	debugNodesHeaderAttributes:   {"Attributes", false},
	debugNodesHeaderEnvironment:  {"Environment", false},
	debugNodesHeaderArguments:    {"Arguments", false},
	debugNodesHeaderTag:          {"Tag", true},
	debugNodesHeaderRevision:     {"Revision", true},
	debugNodesHeaderTime:         {"Time", true},
	debugNodesHeaderType:         {"Type", true},
	debugNodesHeaderPlatform:     {"Platform", true},
	debugNodesHeaderGoVersion:    {"Go Version", true},
	debugNodesHeaderCGO:          {"CGO", true},
	debugNodesHeaderDistribution: {"Distribution", true},
	debugNodesHeaderStartedAt:    {"Started at", false},
	debugNodesHeaderUpdatedAt:    {"Updated at", false},
}

func (s *statusServer) DebugNodes(
	ctx context.Context, req *serverpb.DebugNodesRequest,
) (*serverpb.DebugNodesResponse, error) {
	ctx = s.AnnotateCtx(ctx)

	response := &serverpb.DebugNodesResponse{}
	requestedNodeIDs := make(map[roachpb.NodeID]struct{})
	if len(req.NodeIDs) > 0 {
		for _, nodeIDString := range strings.Split(req.NodeIDs, ",") {
			nodeID, _, err := s.parseNodeID(nodeIDString)
			if err != nil {
				return nil, grpc.Errorf(codes.InvalidArgument, errors.Wrapf(err,
					"could not parse node_ids parameter, it must be a comma separated list of node ids: %s",
					req.NodeIDs,
				).Error())
			}
			requestedNodeIDs[nodeID] = struct{}{}
		}
		var requestedNodeIDValues []roachpb.NodeID
		for requestedNodeID := range requestedNodeIDs {
			requestedNodeIDValues = append(requestedNodeIDValues, requestedNodeID)
		}
		sort.Slice(requestedNodeIDValues, func(i, j int) bool {
			return requestedNodeIDValues[i] < requestedNodeIDValues[j]
		})
		requestedNodeIDStrings := make([]string, len(requestedNodeIDValues))
		for i, requestedNodeID := range requestedNodeIDValues {
			requestedNodeIDStrings[i] = fmt.Sprintf("n%d", requestedNodeID)
		}
		response.Filters = []string{fmt.Sprintf("only nodes: %s", strings.Join(requestedNodeIDStrings, ","))}
	}
	var localityRegex *regexp.Regexp
	if len(req.Locality) > 0 {
		var err error
		localityRegex, err = regexp.Compile(req.Locality)
		if err != nil {
			return nil, grpc.Errorf(codes.InvalidArgument, errors.Wrapf(err,
				"could not compile regex for locality parameter: %s",
				req.Locality,
			).Error())
		}
		response.Filters = append(response.Filters,
			fmt.Sprintf("locality regex: %s", localityRegex.String()))
	}

	resp, err := s.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, grpc.Errorf(codes.Internal,
			errors.Wrapf(err, "could not retrieve node statuses").Error())
	}

	var mostRecentUpdatedAt time.Time
	type nodeIDItem struct {
		id       roachpb.NodeID
		locality string
		address  string
	}
	var nodeIDs []nodeIDItem
	nodeStatuses := make(map[roachpb.NodeID]status.NodeStatus)
	for _, nodeStatus := range resp.Nodes {
		nodeStatuses[nodeStatus.Desc.NodeID] = nodeStatus
		if updatedAt := time.Unix(0, nodeStatus.UpdatedAt); updatedAt.After(mostRecentUpdatedAt) {
			mostRecentUpdatedAt = updatedAt
		}
		if len(requestedNodeIDs) > 0 {
			// If a subset of nodes have been requested, skip all those that are not
			// in that list.
			if _, ok := requestedNodeIDs[nodeStatus.Desc.NodeID]; !ok {
				continue
			}
		}
		if localityRegex != nil {
			if !localityRegex.MatchString(nodeStatus.Desc.Locality.String()) {
				continue
			}
		}

		nodeIDs = append(nodeIDs, nodeIDItem{
			id:       nodeStatus.Desc.NodeID,
			locality: nodeStatus.Desc.Locality.String(),
			address:  nodeStatus.Desc.Address.AddressField,
		})
	}

	// Find all outdated connections.
	deadNodes := make(map[roachpb.NodeID]status.NodeStatus)
	cutoff := mostRecentUpdatedAt.Add(-timeUntilStoreDead.Get())
	for nodeID, nodeStatus := range nodeStatuses {
		if updatedAt := time.Unix(0, nodeStatus.UpdatedAt); updatedAt.Before(cutoff) {
			deadNodes[nodeID] = nodeStatus
		}
	}

	// Sort all the nodes.
	sort.Slice(nodeIDs, func(i, j int) bool {
		if nodeIDs[i].locality == nodeIDs[j].locality {
			return nodeIDs[i].id < nodeIDs[j].id
		}
		return nodeIDs[i].locality < nodeIDs[j].locality
	})

	// Add the headers.
	response.Rows = make([]serverpb.DebugRow, len(debugNodesHeaders))
	for i, header := range debugNodesHeaders {
		response.Rows[i].Cells = append(response.Rows[i].Cells, serverpb.DebugCell{
			Title:   header.title,
			Values:  []string{header.title},
			Classes: []string{debugClassCellLeftHeader, debugClassCellShort},
		})
	}

	addOutputWithTitle := func(i int, value, title string) {
		response.Rows[i].Cells = append(response.Rows[i].Cells, serverpb.DebugCell{
			Title:  title,
			Values: []string{value},
		})
	}

	addOutput := func(i int, value string) {
		addOutputWithTitle(i, value, value)
	}

	addOutputs := func(i int, values []string) {
		response.Rows[i].Cells = append(response.Rows[i].Cells, serverpb.DebugCell{
			Title:  strings.Join(values, "\n"),
			Values: values,
		})
	}

	for _, item := range nodeIDs {
		nodeID := item.id
		if _, nodeDead := deadNodes[nodeID]; nodeDead {
			continue
		}
		nodeStatus, ok := nodeStatuses[nodeID]
		if !ok {
			response.Failures = append(response.Failures, serverpb.DebugFailure{
				NodeID:       nodeID,
				ErrorMessage: "no node status available",
			})
			continue
		}
		addOutput(debugNodesHeaderNode, fmt.Sprintf("n%d", nodeID))
		addOutput(debugNodesHeaderAddress, nodeStatus.Desc.Address.String())
		netAddr, err := nodeStatus.Desc.Address.Resolve()
		if err != nil {
			response.Failures = append(response.Failures, serverpb.DebugFailure{
				NodeID:       nodeID,
				ErrorMessage: "could not resolve ip",
			})
			addOutput(debugNodesHeaderIP, "")
		} else {
			addOutput(debugNodesHeaderIP, netAddr.String())
		}
		addOutput(debugNodesHeaderLocality, nodeStatus.Desc.Locality.String())
		addOutputs(debugNodesHeaderAttributes, nodeStatus.Desc.Attrs.Attrs)
		addOutputs(debugNodesHeaderEnvironment, nodeStatus.Env)
		addOutputs(debugNodesHeaderArguments, nodeStatus.Args)
		addOutput(debugNodesHeaderTag, nodeStatus.BuildInfo.Tag)
		addOutput(debugNodesHeaderRevision, nodeStatus.BuildInfo.Revision)
		addOutput(debugNodesHeaderTime, nodeStatus.BuildInfo.Time)
		addOutput(debugNodesHeaderType, nodeStatus.BuildInfo.Type)
		addOutput(debugNodesHeaderPlatform, nodeStatus.BuildInfo.Platform)
		addOutput(debugNodesHeaderGoVersion, nodeStatus.BuildInfo.GoVersion)
		addOutput(debugNodesHeaderCGO, nodeStatus.BuildInfo.CgoCompiler)
		addOutput(debugNodesHeaderDistribution, nodeStatus.BuildInfo.Distribution)
		startedAt := time.Unix(0, nodeStatus.StartedAt).String()
		addOutputWithTitle(debugNodesHeaderStartedAt, startedAt, fmt.Sprintf("%d\n%s", nodeStatus.StartedAt, startedAt))
		updatedAt := time.Unix(0, nodeStatus.UpdatedAt).String()
		addOutputWithTitle(debugNodesHeaderUpdatedAt, updatedAt, fmt.Sprintf("%d\n%s", nodeStatus.UpdatedAt, updatedAt))
	}

	// Check for values that don't all match
	for i, header := range debugNodesHeaders {
		if !header.check {
			continue
		}
		for j := 2; j < len(response.Rows[i].Cells); j++ {
			if response.Rows[i].Cells[1].Title != response.Rows[i].Cells[j].Title {
				response.Rows[i].Cells[0].Classes = append(response.Rows[i].Cells[0].Classes,
					debugClassWarning)
				break
			}
		}
	}

	return response, nil
}
