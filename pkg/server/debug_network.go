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
	"html/template"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
)

const (
	// The preceding space allows for simple string concatenations on classes.
	debugNetworkClassHeaderTop    = " header top"
	debugNetworkClassHeaderLeft   = " header left"
	debugNetworkClassDead         = " dead"
	debugNetworkClassSpacer       = " spacer"
	debugNetworkClassNoConnection = " no-connection"
	debugNetworkClassSelf         = " self"
	debugNetworkClassStddevPlus2  = " stddev-plus-2"
	debugNetworkClassStddevPlus1  = " stddev-plus-1"
	debugNetworkClassStddevEven   = " stddev-even"
	debugNetworkClassStddevMinus1 = " stddev-minus-1"
	debugNetworkClassStddevMinus2 = " stddev-minus-2"

	oldStoreCutoff = time.Minute
)

// Returns an HTML page displaying information about networking issues between
// nodes.
func (s *statusServer) handleDebugNetwork(w http.ResponseWriter, r *http.Request) {
	ctx := s.AnnotateCtx(r.Context())
	w.Header().Add("Content-type", "text/html")

	resp, err := s.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		http.Error(w, "could not retrieve node statuses", http.StatusInternalServerError)
	}

	data := debugNetwork{
		DeadNodes: make(map[roachpb.NodeID]debugNetworkDeadNode),
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
		nodeIDs = append(nodeIDs, nodeIDItem{
			id:       nodeStatus.Desc.NodeID,
			locality: nodeStatus.Desc.Locality.String(),
			address:  nodeStatus.Desc.Address.AddressField,
		})
	}

	// Find all outdated connections.
	cutoff := mostRecentUpdatedAt.Add(-oldStoreCutoff)
	for nodeID, nodeStatus := range nodeStatuses {
		if updatedAt := time.Unix(0, nodeStatus.UpdatedAt); updatedAt.Before(cutoff) {
			data.DeadNodes[nodeID] = debugNetworkDeadNode{
				Address:     nodeStatus.Desc.Address.AddressField,
				Locality:    nodeStatus.Desc.Locality.String(),
				LastUpdated: updatedAt.String(),
			}
			continue
		}
	}

	// Only calculate statistics on recent, non-self connections.
	var latencyCount int
	var latencySum, latencySquareSum int64
	for nodeID, nodeStatus := range nodeStatuses {
		if _, ok := data.DeadNodes[nodeID]; !ok {
			for otherNodeID, latency := range nodeStatus.Latencies {
				if _, ok := data.DeadNodes[otherNodeID]; !ok && otherNodeID != nodeStatus.Desc.NodeID {
					latencyCount++
					latencySum += latency
					latencySquareSum += latency * latency
				}
			}
		}
	}

	var mean, stddev float64
	if latencyCount > 0 {
		mean = float64(latencySum) / float64(latencyCount)
		if latencyCount > 1 {
			stddev = math.Sqrt(float64(latencySquareSum)/float64(latencyCount-1) - mean*mean)
		}
	}
	stddevPlus1 := mean + stddev
	stddevPlus2 := stddevPlus1 + stddev
	stddevMinus1 := mean - stddev
	stddevMinus2 := stddevMinus1 - stddev

	sort.Slice(nodeIDs, func(i, j int) bool {
		if nodeIDs[i].locality == nodeIDs[j].locality {
			return nodeIDs[i].id < nodeIDs[j].id
		}
		return nodeIDs[i].locality < nodeIDs[j].locality
	})

	data.Latencies = make([][]*debugRangeOutput, len(nodeIDs)+1)
	data.Latencies[0] = make([]*debugRangeOutput, len(nodeIDs)+1)
	data.Latencies[0][0] = &debugRangeOutput{Class: debugNetworkClassSpacer}
	for i, item := range nodeIDs {
		nodeID := item.id
		var nodeHeaderClass string
		_, nodeDead := data.DeadNodes[nodeID]
		if nodeDead {
			nodeHeaderClass = debugNetworkClassDead
		}
		// Add the top header.
		data.Latencies[0][i+1] = &debugRangeOutput{
			Value: fmt.Sprintf("n%d", nodeID),
			Title: fmt.Sprintf("n%d\n%s\n%s", nodeID, item.address, item.locality),
			Class: nodeHeaderClass + debugNetworkClassHeaderTop,
		}
		// Create the row and add the left header.
		data.Latencies[i+1] = make([]*debugRangeOutput, len(nodeIDs)+1)
		data.Latencies[i+1][0] = &debugRangeOutput{
			Value: fmt.Sprintf("n%d", nodeID),
			Title: fmt.Sprintf("n%d\n%s\n%s", nodeID, item.address, item.locality),
			Class: nodeHeaderClass + debugNetworkClassHeaderLeft,
		}
		nodeStatus, ok := nodeStatuses[nodeID]
		if !ok {
			data.Failures = append(data.Failures,
				fmt.Sprintf("no node status available for n%d", nodeID))
			continue
		}
		for j, otherItem := range nodeIDs {
			otherNodeID := otherItem.id
			// Self latency
			if nodeID == otherNodeID {
				data.Latencies[i+1][j+1] = &debugRangeOutput{
					Title: fmt.Sprintf("n%d -> n%d\n", nodeID, otherNodeID),
					Class: debugNetworkClassSelf,
				}
				continue
			}
			otherNodeStatus, ok := nodeStatuses[otherNodeID]
			if !ok {
				// The error cases have already been handled.
				continue
			}
			_, otherNodeDead := data.DeadNodes[otherNodeID]
			if lat, ok := nodeStatus.Latencies[otherNodeID]; ok && !nodeDead && !otherNodeDead {
				// Valid connection
				var class string
				switch {
				case float64(lat) > stddevPlus2:
					class = debugNetworkClassStddevPlus2
				case float64(lat) > stddevPlus1:
					class = debugNetworkClassStddevPlus1
				case float64(lat) < stddevMinus2:
					class = debugNetworkClassStddevMinus2
				case float64(lat) < stddevMinus1:
					class = debugNetworkClassStddevMinus1
				default:
					class = debugNetworkClassStddevEven
				}
				ms := float64(lat) / float64(time.Millisecond)
				data.Latencies[i+1][j+1] = &debugRangeOutput{
					Value: fmt.Sprintf("%.1fms", ms),
					Title: fmt.Sprintf("n%d -> n%d\n%fms", nodeID, otherNodeID, ms),
					Class: class,
				}
				continue
			}
			// No Connection
			data.Latencies[i+1][j+1] = &debugRangeOutput{
				Value: "X",
				Title: fmt.Sprintf("n%d -> n%d\nno connection", nodeID, otherNodeID),
				Class: debugNetworkClassNoConnection,
			}
			// Don't add dead nodes to the no connections list.
			if !nodeDead && !otherNodeDead {
				data.NoConnections = append(data.NoConnections, debugNetworkNoConnection{
					FromNodeID:   nodeID,
					FromAddress:  nodeStatus.Desc.Address.AddressField,
					FromLocality: nodeStatus.Desc.Locality.String(),
					ToNodeID:     otherNodeID,
					ToAddress:    otherNodeStatus.Desc.Address.AddressField,
					ToLocality:   otherNodeStatus.Desc.Locality.String(),
				})
			}
		}
	}

	// Add in the legend.
	data.Legend = make([][]*debugRangeOutput, 2)
	data.Legend[0] = []*debugRangeOutput{
		{Class: debugNetworkClassHeaderTop, Value: "< -2 stddev"},
		{Class: debugNetworkClassHeaderTop, Value: "< -1 stddev"},
		{Class: debugNetworkClassHeaderTop, Value: "mean"},
		{Class: debugNetworkClassHeaderTop, Value: "> +1 stddev"},
		{Class: debugNetworkClassHeaderTop, Value: "> +2 stddev"},
	}
	createLegendOutput := func(lat float64, class string) *debugRangeOutput {
		return &debugRangeOutput{
			Class: class,
			Value: fmt.Sprintf("%.2fms", lat/float64(time.Millisecond)),
			Title: fmt.Sprintf("%fms", lat/float64(time.Millisecond)),
		}
	}
	data.Legend[1] = []*debugRangeOutput{
		createLegendOutput(stddevMinus2, debugNetworkClassStddevMinus2),
		createLegendOutput(stddevMinus1, debugNetworkClassStddevMinus1),
		createLegendOutput(mean, debugNetworkClassStddevEven),
		createLegendOutput(stddevPlus1, debugNetworkClassStddevPlus1),
		createLegendOutput(stddevPlus2, debugNetworkClassStddevPlus2),
	}

	t, err := template.New("webpage").Parse(debugNetworkTemplate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type debugNetworkNoConnection struct {
	FromNodeID   roachpb.NodeID
	FromAddress  string
	FromLocality string
	ToNodeID     roachpb.NodeID
	ToAddress    string
	ToLocality   string
}

type debugNetworkDeadNode struct {
	Address     string
	Locality    string
	LastUpdated string
}

type debugNetwork struct {
	Failures      []string
	NoConnections []debugNetworkNoConnection
	Latencies     [][]*debugRangeOutput
	Legend        [][]*debugRangeOutput
	DeadNodes     map[roachpb.NodeID]debugNetworkDeadNode
}

const debugNetworkTemplate = `
<!DOCTYPE html>
<HTML>
  <HEAD>
  	<META CHARSET="UTF-8"/>
    <TITLE>Network Diagnostics</TITLE>
    <STYLE>
      body {
        font-family: "Helvetica Neue", Helvetica, Arial;
        font-size: 14px;
        line-height: 20px;
        font-weight: 400;
        color: #3b3b3b;
        -webkit-font-smoothing: antialiased;
        font-smoothing: antialiased;
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
        padding: 6px 12px;
        display: table-cell;
        height: 20px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
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
      }
      .cell.header.left{
        text-align: right;
      }
      .cell.header.dead {
        color: orangered;
      }
      .cell.spacer{
        border: none;
        background-color: #e4e4e4;
      }
      .cell.no-connection {
        color: darkred;
      }
      .cell.self {
        background-color: #e9e9e9;
      }
      .stddev-plus-2 {
        background-color: rgba(255, 0, 0, 0.4);
      }
      .stddev-plus-1 {
        background-color: rgba(255, 0, 0, 0.2);
      }
      .stddev-even {
        background-color: white;
      }
      .stddev-minus-1 {
        background-color: rgba(0, 255, 0, 0.2);
      }
      .stddev-minus-2 {
        background-color: rgba(0, 255, 0, 0.4);
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
        color: #ffffff;
        border: none;
      }
      .list-row:first-of-type .list-cell.no-connection {
        background-color: #ea6153;
      }
      .list-cell:first-of-type .list-cell.failure {
        color: black;
        background-color: white;
      }
      .list-cell.failure {
        color: black;
      }
    </STYLE>
  </HEAD>
  <BODY>
    <DIV CLASS="wrapper">
      <H1>Network Debug</H1>
      <H2>Latencies</H2>
      <DIV CLASS="table">
        {{- range $_, $row := $.Latencies}}
          <DIV CLASS="row">
            {{- range $_, $cell := $row}}
              <DIV CLASS="cell {{$cell.Class}}" TITLE="{{$cell.Title}}">
                {{$cell.Value}}
              </DIV>
            {{- end}}
          </DIV>
        {{- end}}
      </DIV>
      <H3>Legend</H3>
      <DIV CLASS="table">
        {{- range $_, $row := $.Legend}}
          <DIV CLASS="row">
            {{- range $_, $cell := $row}}
              <DIV CLASS="cell {{$cell.Class}}" TITLE="{{$cell.Title}}">
                {{$cell.Value}}
              </DIV>
            {{- end}}
          </DIV>
        {{- end}}
      </DIV>
      {{- if $.DeadNodes }}
        <H2>Dead Nodes</H2>
        <DIV CLASS="list-table">
          <DIV CLASS="list-row">
            <DIV CLASS="list-cell no-connection default">Node</DIV>
            <DIV CLASS="list-cell no-connection default">Address</DIV>
            <DIV CLASS="list-cell no-connection default">Locality</DIV>
            <DIV CLASS="list-cell no-connection default">Last Updated</DIV>
          </DIV>
          {{- range $id, $dead := $.DeadNodes}}
            <DIV CLASS="list-row">
              <DIV CLASS="list-cell no-connection">n{{$id}}</DIV>
              <DIV CLASS="list-cell no-connection">{{$dead.Address}}</DIV>
              <DIV CLASS="list-cell no-connection">{{$dead.Locality}}</DIV>
              <DIV CLASS="list-cell no-connection">{{$dead.LastUpdated}}</DIV>
            </DIV>
          {{- end}}
        </DIV>
      {{- end}}
      {{- if $.NoConnections }}
        <H2>No Connection</H2>
        <DIV CLASS="list-table">
          <DIV CLASS="list-row">
            <DIV CLASS="list-cell no-connection">From Node</DIV>
            <DIV CLASS="list-cell no-connection">From Address</DIV>
            <DIV CLASS="list-cell no-connection">From Locality</DIV>
            <DIV CLASS="list-cell no-connection">To Node</DIV>
            <DIV CLASS="list-cell no-connection">To Address</DIV>
            <DIV CLASS="list-cell no-connection">To Locality</DIV>
          </DIV>
          {{- range $_, $lat := $.NoConnections}}
            <DIV CLASS="list-row">
              <DIV CLASS="list-cell no-connection">n{{$lat.FromNodeID}}</DIV>
              <DIV CLASS="list-cell no-connection">{{$lat.FromAddress}}</DIV>
              <DIV CLASS="list-cell no-connection">{{$lat.FromLocality}}</DIV>
              <DIV CLASS="list-cell no-connection">n{{$lat.ToNodeID}}</DIV>
              <DIV CLASS="list-cell no-connection">{{$lat.ToAddress}}</DIV>
              <DIV CLASS="list-cell no-connection">{{$lat.ToLocality}}</DIV>
            </DIV>
          {{- end}}
        </DIV>
      {{- end}}
      {{- if $.Failures }}
        <H2>Failures</H2>
        <DIV CLASS="list-table">
          {{- range $_, $f := $.Failures}}
            <DIV CLASS="list-row">
              <DIV CLASS="list-cell failure">n{{$f}}</DIV>
            </DIV>
          {{- end}}
        </DIV>
      {{- end}}
    </DIV>
  </BODY>
</HTML>
`
