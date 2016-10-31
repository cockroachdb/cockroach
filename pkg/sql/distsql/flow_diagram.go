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
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

type diagramCellType interface {
	// summary produces a title and an arbitrary number of lines that describe a
	// "cell" in a diagram node (input sync, processor core, or output router).
	summary() (title string, details []string)
}

func (ord *Ordering) diagramString() string {
	var buf bytes.Buffer
	for i, c := range ord.Columns {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "%d", c.ColIdx)
		if c.Direction == Ordering_Column_DESC {
			buf.WriteByte('-')
		} else {
			buf.WriteByte('+')
		}
	}
	return buf.String()
}

func colListStr(cols []uint32) string {
	var buf bytes.Buffer
	for i, c := range cols {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "%d", c)
	}
	return buf.String()
}

func (*NoopCoreSpec) summary() (string, []string) {
	return "No-op", []string{}
}

func (tr *TableReaderSpec) summary() (string, []string) {
	index := "primary"
	if tr.IndexIdx > 0 {
		index = tr.Table.Indexes[tr.IndexIdx-1].Name
	}
	details := []string{
		fmt.Sprintf("%s@%s", index, tr.Table.Name),
		colListStr(tr.OutputColumns),
	}
	if tr.Filter.Expr != "" {
		details = append(details, tr.Filter.Expr)
	}
	// TODO(radu): a summary of the spans
	return "TableReader", details
}

func (jr *JoinReaderSpec) summary() (string, []string) {
	index := "primary"
	if jr.IndexIdx > 0 {
		index = jr.Table.Indexes[jr.IndexIdx-1].Name
	}
	details := []string{
		fmt.Sprintf("%s@%s", index, jr.Table.Name),
		colListStr(jr.OutputColumns),
	}
	if jr.Filter.Expr != "" {
		details = append(details, jr.Filter.Expr)
	}
	return "JoinReader", details
}

func (hj *HashJoinerSpec) summary() (string, []string) {
	details := []string{
		fmt.Sprintf(
			"ON left(%s)=right(%s)", colListStr(hj.LeftEqColumns), colListStr(hj.RightEqColumns),
		),
		colListStr(hj.OutputColumns),
	}
	if hj.Expr.Expr != "" {
		details = append(details, hj.Expr.Expr)
	}
	return "HashJoiner", details
}

// TODO(radu): implement summary() for other core types.

func (is *InputSyncSpec) summary() (string, []string) {
	switch is.Type {
	case InputSyncSpec_UNORDERED:
		return "unordered", []string{}
	case InputSyncSpec_ORDERED:
		return "ordered", []string{is.Ordering.diagramString()}
	default:
		return "unknown", []string{}
	}
}

func (r *OutputRouterSpec) summary() (string, []string) {
	switch r.Type {
	case OutputRouterSpec_MIRROR:
		return "mirror", []string{}
	case OutputRouterSpec_BY_HASH:
		return "by hash", []string{colListStr(r.HashColumns)}
	case OutputRouterSpec_BY_RANGE:
		return "by range", []string{}
	default:
		return "unknown", []string{}
	}
}

type diagramCell struct {
	Title   string   `json:"title"`
	Details []string `json:"details"`
}

type diagramProcessor struct {
	NodeIdx int           `json:"nodeIdx"`
	Inputs  []diagramCell `json:"inputs"`
	Core    diagramCell   `json:"core"`
	Outputs []diagramCell `json:"outputs"`
}

type diagramEdge struct {
	SourceProc   int `json:"sourceProc"`
	SourceOutput int `json:"sourceOutput"`
	DestProc     int `json:"destProc"`
	DestInput    int `json:"destInput"`
}

type diagramData struct {
	NodeNames  []string           `json:"nodeNames"`
	Processors []diagramProcessor `json:"processors"`
	Edges      []diagramEdge      `json:"edges"`
}

func generateDiagramData(flows []FlowSpec, nodeNames []string) (diagramData, error) {
	d := diagramData{NodeNames: nodeNames}

	// inPorts maps streams to their "destination" attachment point. Only DestProc
	// and DestInput are set in each diagramEdge value.
	inPorts := make(map[StreamID]diagramEdge)
	syncResponseNode := -1

	pIdx := 0
	for n := range flows {
		for _, p := range flows[n].Processors {
			proc := diagramProcessor{NodeIdx: n}
			proc.Core.Title, proc.Core.Details = p.Core.GetValue().(diagramCellType).summary()

			// We need explicit synchronizers if we have multiple inputs, or if the
			// one input has multiple input streams.
			if len(p.Input) > 1 || (len(p.Input) == 1 && len(p.Input[0].Streams) > 1) {
				proc.Inputs = make([]diagramCell, len(p.Input))
				for i, s := range p.Input {
					proc.Inputs[i].Title, proc.Inputs[i].Details = s.summary()
				}
			} else {
				proc.Inputs = []diagramCell{}
			}

			// Add entries in the map for the inputs.
			for i, input := range p.Input {
				val := diagramEdge{
					DestProc: pIdx,
				}
				if len(proc.Inputs) > 0 {
					val.DestInput = i + 1
				}
				for _, stream := range input.Streams {
					inPorts[stream.StreamID] = val
				}
			}

			for _, r := range p.Output {
				for _, o := range r.Streams {
					if o.Type == StreamEndpointSpec_SYNC_RESPONSE {
						if syncResponseNode != -1 && syncResponseNode != n {
							return diagramData{}, errors.Errorf("multiple nodes with SyncResponse")
						}
						syncResponseNode = n
					}
				}
			}

			// We need explicit routers if we have multiple outputs, or if the one
			// output has multiple input streams.
			if len(p.Output) > 1 || (len(p.Output) == 1 && len(p.Output[0].Streams) > 1) {
				proc.Outputs = make([]diagramCell, len(p.Output))
				for i, r := range p.Output {
					proc.Outputs[i].Title, proc.Outputs[i].Details = r.summary()
				}
			} else {
				proc.Outputs = []diagramCell{}
			}
			d.Processors = append(d.Processors, proc)
			pIdx++
		}
	}

	if syncResponseNode != -1 {
		d.Processors = append(d.Processors, diagramProcessor{
			NodeIdx: syncResponseNode,
			Core:    diagramCell{Title: "Response", Details: []string{}},
			Inputs:  []diagramCell{},
			Outputs: []diagramCell{},
		})
	}

	// Produce the edges.
	pIdx = 0
	for n := range flows {
		for _, p := range flows[n].Processors {
			for i, output := range p.Output {
				srcOutput := 0
				if len(d.Processors[pIdx].Outputs) > 0 {
					srcOutput = i + 1
				}
				for _, o := range output.Streams {
					edge := diagramEdge{
						SourceProc:   pIdx,
						SourceOutput: srcOutput,
					}
					if o.Type == StreamEndpointSpec_SYNC_RESPONSE {
						edge.DestProc = len(d.Processors) - 1
					} else {
						to, ok := inPorts[o.StreamID]
						if !ok {
							return diagramData{}, errors.Errorf("stream %d has no destination", o.StreamID)
						}
						edge.DestProc = to.DestProc
						edge.DestInput = to.DestInput
					}
					d.Edges = append(d.Edges, edge)
				}
			}
			pIdx++
		}
	}
	return d, nil
}

// GeneratePlanDiagram generates the json data for a flow diagram.  There should
// be one FlowSpec per node. The function assumes that StreamIDs are unique
// across all flows.
func GeneratePlanDiagram(flows []FlowSpec, nodeNames []string, w io.Writer) error {
	d, err := generateDiagramData(flows, nodeNames)
	if err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(d)
}
