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
	"bytes"
	"fmt"
	"html/template"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
)

const (
	// The preceding space allows for simple string concatenations on classes.
	debugNodesClassHeader        = " header"
	debugNodesClassHeaderWarning = " warning"
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

// Returns an HTML page displaying information about nodes status.
func (s *statusServer) handleDebugNodes(w http.ResponseWriter, r *http.Request) {
	ctx := s.AnnotateCtx(r.Context())
	w.Header().Add("Content-type", "text/html")

	nodeIDsString := r.URL.Query().Get("node_ids")
	requestedNodeIDs := make(map[roachpb.NodeID]struct{})

	var data debugNodes
	if len(nodeIDsString) > 0 {
		var nodeFilter bytes.Buffer
		nodeFilter.WriteString("Only nodes: ")
		for i, nodeIDString := range strings.Split(nodeIDsString, ",") {
			nodeID, _, err := s.parseNodeID(nodeIDString)
			if err != nil {
				http.Error(w, errors.Wrapf(err,
					"could not parse node_ids parameter, it must be a comma separated list of node ids: %s",
					nodeIDsString,
				).Error(), http.StatusBadRequest)
				return
			}
			requestedNodeIDs[nodeID] = struct{}{}
			if i > 0 {
				nodeFilter.WriteString(",")
			}
			nodeFilter.WriteString(nodeID.String())
		}
		data.Filters = append(data.Filters, nodeFilter.String())
	}
	localityString := r.URL.Query().Get("locality")
	var localityRegex *regexp.Regexp
	if len(localityString) > 0 {
		var err error
		localityRegex, err = regexp.Compile(localityString)
		if err != nil {
			http.Error(w, errors.Wrapf(err, "could not compile regex for locality parameter: %s",
				localityString).Error(), http.StatusBadRequest)
		}
		data.Filters = append(data.Filters, fmt.Sprintf("Locality Regex: %s", localityString))
	}

	resp, err := s.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		http.Error(w, "could not retrieve node statuses", http.StatusInternalServerError)
		return
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
	data.Data = make([][]*debugOutput, len(debugNodesHeaders))
	for i, header := range debugNodesHeaders {
		data.Data[i] = append(data.Data[i], &debugOutput{
			Title: header.title,
			Value: header.title,
			Class: debugNodesClassHeader,
		})
	}

	addOutputWithTitle := func(i int, value, title string) {
		data.Data[i] = append(data.Data[i], &debugOutput{
			Title: title,
			Value: value,
		})
	}

	addOutput := func(i int, value string) {
		addOutputWithTitle(i, value, value)
	}

	addOutputs := func(i int, values []string) {
		data.Data[i] = append(data.Data[i], &debugOutput{
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
			data.Failures = append(data.Failures,
				fmt.Sprintf("no node status available for n%d", nodeID))
			continue
		}
		addOutput(debugNodesHeaderNode, fmt.Sprintf("n%d", nodeID))
		addOutput(debugNodesHeaderAddress, nodeStatus.Desc.Address.String())
		netAddr, err := nodeStatus.Desc.Address.Resolve()
		if err != nil {
			data.Failures = append(data.Failures, fmt.Sprintf("could not resolve ip for n%d", nodeID))
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
		for j := 2; j < len(data.Data[i]); j++ {
			if data.Data[i][1].Title != data.Data[i][j].Title {
				data.Data[i][0].Class += debugNodesClassHeaderWarning
				break
			}
		}
	}

	t, err := template.New("webpage").Parse(debugNodesTemplate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type debugNodes struct {
	Failures []string
	Data     [][]*debugOutput
	Filters  []string
}

const debugNodesTemplate = `
<!DOCTYPE html>
<HTML>
  <HEAD>
  	<META CHARSET="UTF-8"/>
    <TITLE>Node Diagnostics</TITLE>
    <STYLE>
      body {
        font-family: "Helvetica Neue", Helvetica, Arial;
        font-size: 12px;
        line-height: 20px;
        font-weight: 400;
        color: #3b3b3b;
        -webkit-font-smoothing: antialiased;
        background-color: #e4e4e4;
      }
      .wrapper {
        margin: 0 auto;
        padding: 0 40px;
      }
      .table {
        margin: 0 0 40px 0;
        display: table;
      }
      .row {
        display: table-row;
        background-color: white;
      }
      .cell {
        padding: 1px 10px;
        display: table-cell;
        height: 20px;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 200px;
        border-width: 1px 1px 0 0;
        border-color: rgba(0, 0, 0, 0.1);
        border-style: solid;
      }
      .cell.header{
        font-weight: 900;
        color: #ffffff;
        background-color: #2980b9;
        text-overflow: clip;
        width: 1px;
        text-align: right;
      }
      .cell.header.warning{
        color: yellow;
      }
      .list-table {
        margin: 0 0 40px 0;
        display: table;
      }
      .list-row {
        display: table-row;
        background-color: white;
      }
      .list-cell {
        padding: 6px 12px;
        display: table-cell;
        height: 20px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        border-width: 1px 1px 0 0;
        border-color: rgba(0, 0, 0, 0.1);
        border-style: solid;
      }
      .list-row:first-of-type .list-cell {
        font-weight: 900;
        color: white;
        border: none;
      }
      .failure-cell {
        padding: 6px 12px;
        display: table-cell;
        height: 20px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        border-width: 1px 1px 0 0;
        border-color: rgba(0, 0, 0, 0.1);
        border-style: solid;
        font-weight: 900;
        color: black;
        background-color: white;
      }
    </STYLE>
  </HEAD>
  <BODY>
    <DIV CLASS="wrapper">
      <H1>Node Diagnostics Page</H1>
      {{- if $.Filters}}
        <H2>Filters</H2>
        {{- range $_, $filter := $.Filters}}
          <DIV>• {{$filter}}</DIV>
        {{- end}}
      {{- end}}
      <H2>Nodes</H2>
      <DIV CLASS="table">
        {{- range $_, $row := $.Data}}
          <DIV CLASS="row">
            {{- range $_, $cell := $row}}
              <DIV CLASS="cell {{$cell.Class}}" TITLE="{{$cell.Title}}">
                {{- if $cell.Values}}
                  {{- range $_, $value := $cell.Values}}
                    {{$value}}<BR/>
                  {{- end}}
                {{- else}}
                  {{$cell.Value}}
                {{- end}}
              </DIV>
            {{- end}}
          </DIV>
        {{- end}}
      </DIV>
      {{- if $.Failures }}
        <H2>Failures</H2>
        <DIV CLASS="table">
          {{- range $_, $f := $.Failures}}
            <DIV CLASS="row">
              <DIV CLASS="failure-cell">{{$f}}</DIV>
            </DIV>
          {{- end}}
        </DIV>
      {{- end}}
    </DIV>
  </BODY>
</HTML>
`
