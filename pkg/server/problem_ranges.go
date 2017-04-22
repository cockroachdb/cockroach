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
	"net/http"
	"sort"
	"strconv"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// Returns an HTML page displaying information about all the problem ranges in
// a node or the full cluster.
func (s *statusServer) handleProblemRanges(w http.ResponseWriter, r *http.Request) {
	ctx := s.AnnotateCtx(r.Context())
	w.Header().Add("Content-type", "text/html")
	nodeIDString := r.URL.Query().Get("node_id")

	data := debugProblemRangeData{}
	if len(nodeIDString) > 0 {
		nodeIDInt, err := strconv.ParseInt(nodeIDString, 10, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		nodeID := roachpb.NodeID(nodeIDInt)
		data.NodeID = &nodeID
	}

	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.RangesResponse
		err    error
	}

	isLiveMap := s.nodeLiveness.GetIsLiveMap()
	if data.NodeID != nil {
		// If there is a specific nodeID requested, limited the responses to
		// just this node.
		if !isLiveMap[*data.NodeID] {
			http.Error(w, fmt.Sprintf("n%d is not alive", *data.NodeID), http.StatusInternalServerError)
			return
		}
		isLiveMap = map[roachpb.NodeID]bool{
			*data.NodeID: true,
		}
	}

	numNodes := len(isLiveMap)
	responses := make(chan nodeResponse)
	nodeCtx, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
	defer cancel()
	for nodeID, alive := range isLiveMap {
		if !alive {
			data.Failures = append(data.Failures, serverpb.RangeInfo{
				SourceNodeID: nodeID,
				ErrorMessage: "node liveness reports that the node is not alive",
			})
			numNodes--
			continue
		}
		nodeID := nodeID
		if err := s.stopper.RunAsyncTask(nodeCtx, func(ctx context.Context) {
			status, err := s.dialNode(nodeID)
			var rangesResponse *serverpb.RangesResponse
			if err == nil {
				req := &serverpb.RangesRequest{}
				rangesResponse, err = status.Ranges(ctx, req)
			}
			response := nodeResponse{
				nodeID: nodeID,
				resp:   rangesResponse,
				err:    err,
			}

			select {
			case responses <- response:
				// Response processed.
			case <-ctx.Done():
				// Context completed, response no longer needed.
			}
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	for remainingResponses := numNodes; remainingResponses > 0; remainingResponses-- {
		select {
		case resp := <-responses:
			if resp.err != nil {
				data.Failures = append(data.Failures, serverpb.RangeInfo{
					SourceNodeID: resp.nodeID,
					ErrorMessage: resp.err.Error(),
				})
				continue
			}
			for _, info := range resp.resp.Ranges {
				if len(info.ErrorMessage) != 0 {
					data.Failures = append(data.Failures, info)
					continue
				}
				if info.Problems.Unavailable {
					data.Unavailable = append(data.Unavailable, info.State.Desc.RangeID)
				}
				if info.Problems.LeaderNotLeaseHolder {
					data.LeaderNotLeaseholder = append(data.LeaderNotLeaseholder, info.State.Desc.RangeID)
				}
			}
		case <-ctx.Done():
			http.Error(w, ctx.Err().Error(), http.StatusRequestTimeout)
			return
		}
	}

	if data.NodeID != nil {
		data.Title = fmt.Sprintf("Problem Ranges for Node n%d", *data.NodeID)
	} else {
		data.Title = "Problem Ranges for the Cluster"
	}

	sort.Sort(data.Unavailable)
	sort.Sort(data.LeaderNotLeaseholder)

	t, err := template.New("webpage").Parse(debugProblemRangesTemplate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type debugProblemRangeData struct {
	NodeID               *roachpb.NodeID
	Title                string
	Failures             rangeInfoSlice
	Unavailable          roachpb.RangeIDSlice
	LeaderNotLeaseholder roachpb.RangeIDSlice
}

const debugProblemRangesTemplate = `
<!DOCTYPE html>
<HTML>
  <HEAD>
  	<META CHARSET="UTF-8"/>
    <TITLE>{{$.Title}}</TITLE>
    <STYLE>
      body {
        font-family: "Helvetica Neue", Helvetica, Arial;
        font-size: 14px;
        line-height: 20px;
        font-weight: 400;
        color: #3b3b3b;
        -webkit-font-smoothing: antialiased;
        font-smoothing: antialiased;
        background: #e4e4e4;
      }
      .wrapper {
        margin: 0 auto;
        padding: 0 40px;
      }
      .table {
        margin: 0 0 40px 0;
        display: table;
        width: 100%;
      }
      .row {
        display: table-row;
        background: #f6f6f6;
      }
      .cell:nth-of-type(odd) {
        background: #e9e9e9;
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
      .header.cell{
        font-weight: 900;
        color: #ffffff;
        background: #2980b9;
        text-overflow: clip;
        border: none;
        width: 1px;
        text-align: right;
      }
      .failure-table {
        margin: 0 0 40px 0;
        display: table;
        width: 100%;
      }
      .failure-row {
        display: table-row;
        background: #f6f6f6;
      }
      .failure-row:nth-of-type(odd) {
        background: #e9e9e9;
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
        max-width: 500px;
      }
      .failure-row:first-of-type .failure-cell {
        font-weight: 900;
        color: #ffffff;
        background: #ea6153;
        border: none;
      }
      .failure-cell.small {
          max-width: 1px;
      }
    </STYLE>
  </HEAD>
  <BODY>
    <DIV CLASS="wrapper">
      <H1>{{$.Title}}</H1>
      {{- if $.Failures}}
        <H2>Failures</H2>
        <DIV CLASS="failure-table">
          <DIV CLASS="failure-row">
            <DIV CLASS="failure-cell small">Node</DIV>
            <DIV CLASS="failure-cell small">Store</DIV>
            <DIV CLASS="failure-cell">Error</DIV>
          </DIV>
          {{- range $_, $det := $.Failures}}
            <DIV CLASS="failure-row">
              <DIV CLASS="failure-cell small">n{{$det.SourceNodeID}}</DIV>
              {{- if not (eq $det.SourceStoreID 0)}}
                <DIV CLASS="failure-cell small">n{{$det.SourceStoreID}}</DIV>
              {{- else -}}
                <DIV CLASS="failure-cell">-</DIV>
              {{- end}}
              <DIV CLASS="failure-cell" TITLE="{{$det.ErrorMessage}}">{{$det.ErrorMessage}}</DIV>
            </DIV>
          {{- end}}
        </DIV>
      {{- end}}
      <H2>Unavailable</H2>
      <DIV CLASS="table">
        <DIV CLASS="row">
          {{- if $.Unavailable}}
            {{- range $_, $r := $.Unavailable}}
              <a href="/debug/range?id={{$r}}">{{$r}}</a> &nbsp;
            {{- end}}
          {{- else}}
            None
          {{- end}}
        </DIV>
      </DIV>
      <H2>Raft Leader but Not Lease Holder</H2>
      <DIV CLASS="table">
        <DIV CLASS="row">
          {{- if $.LeaderNotLeaseholder}}
            {{- range $_, $r := $.LeaderNotLeaseholder}}
              <a href="/debug/range?id={{$r}}">{{$r}}</a>
            {{- end}}
          {{- else}}
            None
          {{- end}}
        </DIV>
      </DIV>
    </DIV>
  </BODY>
</HTML>
`
