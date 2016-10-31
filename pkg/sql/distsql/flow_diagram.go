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
	"fmt"
	"html/template"
	"io"
)

type diagramCell interface {
	// lines produces a title and an arbitrary number of lines that describe a
	// "cell" in a diagram node (input sync, processor core, or output router).
	lines() (string, []string)
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

func (*NoopCoreSpec) lines() (string, []string) {
	return "No-op", nil
}

func (tr *TableReaderSpec) lines() (string, []string) {
	index := "primary"
	if tr.IndexIdx > 0 {
		index = tr.Table.Indexes[tr.IndexIdx-1].Name
	}
	lines := []string{
		fmt.Sprintf("%s@%s", index, tr.Table.Name),
		colListStr(tr.OutputColumns),
	}
	if tr.Filter.Expr != "" {
		lines = append(lines, tr.Filter.Expr)
	}
	// TODO(radu): a summary of the spans
	return "TableReader", lines
}

func (jr *JoinReaderSpec) lines() (string, []string) {
	index := "primary"
	if jr.IndexIdx > 0 {
		index = jr.Table.Indexes[jr.IndexIdx-1].Name
	}
	lines := []string{
		fmt.Sprintf("%s@%s", index, jr.Table.Name),
		colListStr(jr.OutputColumns),
	}
	if jr.Filter.Expr != "" {
		lines = append(lines, jr.Filter.Expr)
	}
	return "JoinReader", lines
}

func (hj *HashJoinerSpec) lines() (string, []string) {
	lines := []string{
		fmt.Sprintf(
			"ON left(%s)=right(%s)", colListStr(hj.LeftEqColumns), colListStr(hj.RightEqColumns),
		),
		colListStr(hj.OutputColumns),
	}
	if hj.Expr.Expr != "" {
		lines = append(lines, hj.Expr.Expr)
	}
	return "HashJoiner", lines
}

// TODO(radu): implement node() for other core types.

func (is *InputSyncSpec) lines() (string, []string) {
	switch is.Type {
	case InputSyncSpec_UNORDERED:
		return "unordered", nil
	case InputSyncSpec_ORDERED:
		return "ordered", []string{is.Ordering.diagramString()}
	default:
		return "unknown", nil
	}
}

func (r *OutputRouterSpec) lines() (string, []string) {
	switch r.Type {
	case OutputRouterSpec_MIRROR:
		return "mirror", nil
	case OutputRouterSpec_BY_HASH:
		return "by hash", []string{colListStr(r.HashColumns)}
	case OutputRouterSpec_BY_RANGE:
		return "by range", nil
	default:
		return "unknown", nil
	}
}

// GeneratePlanDiagram generates a diagram of a set of flows in DOT format.
// There should be one `FlowSpec` per node. The function assumes that StreamIDs
// are unique across all specs.
func GeneratePlanDiagram(flows []FlowSpec, nodeNames []string, w io.Writer) error {
	// inPorts[streamID] will be populated with the node:port for the input
	// synchronizer for that stream.
	inPorts := make(map[StreamID]string)

	fmt.Fprintln(w, "digraph G {")
	for n := range flows {
		fmt.Fprintf(w, "subgraph cluster_%d {\n", n)
		fmt.Fprintf(w, "label=\"node %s\"\n", nodeNames[n])

		simpleResponse := false

		for pIdx, p := range flows[n].Processors {
			var info procDiagramInfo
			info.Core.Title, info.Core.ExtraLines = p.Core.GetValue().(diagramCell).lines()
			procName := fmt.Sprintf("p%d_%d", n, pIdx)

			// See if we need explicit synchronizer boxes.
			syncBoxes := false
			for _, s := range p.Input {
				if len(s.Streams) > 1 {
					syncBoxes = true
					break
				}
			}

			for i, input := range p.Input {
				var port string
				if syncBoxes {
					port = fmt.Sprintf("%s:in%d", procName, i)
				} else {
					port = procName + ":core"
				}
				for _, stream := range input.Streams {
					inPorts[stream.StreamID] = port
				}
			}

			// See if we need explicit router boxes.
			routerBoxes := false
			for _, r := range p.Output {
				if len(r.Streams) > 1 {
					routerBoxes = true
				}
				for _, o := range r.Streams {
					if o.Mailbox != nil && o.Mailbox.SimpleResponse {
						simpleResponse = true
					}
				}
			}

			numCols := 1
			if syncBoxes {
				numCols = len(p.Input)*2 + 1
			}
			if routerBoxes {
				v := len(p.Output)*2 + 1
				if numCols < v {
					numCols = v
				}
			}
			info.NumCols = numCols
			info.Core.Port = "core"
			if syncBoxes {
				info.Inputs = make([]cellContents, numCols)
				// We have numCols columns and len(p.Input) boxes we want to space
				// evenly. There are (numCols - len(p.input)) empty columns and
				// len(p.Input)+1 spaces.
				spacing := (numCols - len(p.Input)) / (len(p.Input) + 1)
				for i, s := range p.Input {
					var c cellContents
					c.Title, c.ExtraLines = s.lines()
					c.Port = fmt.Sprintf("in%d", i)
					info.Inputs[spacing+(spacing+1)*i] = c
				}
			}
			if routerBoxes {
				info.Outputs = make([]cellContents, numCols)
				spacing := (numCols - len(p.Output)) / (len(p.Output) + 1)
				for i, r := range p.Output {
					var c cellContents
					c.Title, c.ExtraLines = r.lines()
					c.Port = fmt.Sprintf("out%d", i)
					info.Outputs[spacing+(spacing+1)*i] = c
				}
			}
			fmt.Fprintf(w, "%s [shape=none margin=0 label=<\n", procName)
			if err := info.genHTML(w); err != nil {
				return err
			}
			fmt.Fprintln(w, "> ]")
		}

		if simpleResponse {
			fmt.Fprintln(w, "SimpleResponse")
		}

		fmt.Fprintln(w, "}")
	}

	// Produce the edges.
	for n := range flows {
		for pIdx, p := range flows[n].Processors {
			procName := fmt.Sprintf("p%d_%d", n, pIdx)

			// See if we have explicit router boxes.
			routerBoxes := false
			for _, r := range p.Output {
				if len(r.Streams) > 1 {
					routerBoxes = true
					break
				}
			}

			for i, output := range p.Output {
				var port string
				if routerBoxes {
					port = fmt.Sprintf("%s:out%d", procName, i)
				} else {
					port = procName + ":core"
				}
				for _, o := range output.Streams {
					if o.Mailbox != nil && o.Mailbox.SimpleResponse {
						fmt.Fprintf(w, "%s -> SimpleResponse\n", port)
					} else {
						if to, ok := inPorts[o.StreamID]; !ok {
							fmt.Fprintf(w, "%s -> unknown\n", port)
						} else {
							fmt.Fprintf(w, "%s -> %s\n", port, to)
						}
					}
				}
			}
		}
	}

	fmt.Fprintln(w, "}")
	return nil
}

type cellContents struct {
	Title      string
	ExtraLines []string
	Port       string
}

// procDiagramInfo contains the information needed to generate a HTML label for
// a diagram node.
type procDiagramInfo struct {
	Inputs  []cellContents
	Core    cellContents
	NumCols int
	Outputs []cellContents
}

func (p procDiagramInfo) genHTML(w io.Writer) error {
	return processorHTMLTemplate.Execute(w, p)
}

// processorHTML is a template for creating the HTML label of processor nodes.
// Note that the <TD> ... </TD> lines cannot be broken up: it looks like the
// extra whitespace ends up being displayed.
const processorHTML = `
<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">
{{if .Inputs}}<TR>
{{range .Inputs}}{{if .Title}}
  <TD BORDER="1" BGCOLOR="YELLOW"{{if .Port}} port="{{.Port}}"{{end}}><FONT POINT-SIZE="10">{{.Title}}</FONT>{{range .ExtraLines}}<BR/><FONT POINT-SIZE="9">{{.}}</FONT>{{end}}</TD>
  {{else}}<TD WIDTH="20"></TD>
{{end}}{{end}}
</TR>{{end}}
<TR>
  <TD BORDER="1" COLSPAN="{{.NumCols}}" BGCOLOR="SKYBLUE"{{if .Core.Port}} port="{{.Core.Port}}"{{end}}>{{.Core.Title}}{{range .Core.ExtraLines}}<BR/><FONT POINT-SIZE="10">{{.}}</FONT>{{end}}</TD>
</TR>
{{if .Outputs}}<TR>
{{range .Outputs}}{{if .Title}}
  <TD BORDER="1" BGCOLOR="RED"{{if .Port}} port="{{.Port}}"{{end}}><FONT POINT-SIZE="10">{{.Title}}</FONT>{{range .ExtraLines}}<BR/><FONT POINT-SIZE="9">{{.}}</FONT>{{end}}</TD>
  {{else}}<TD WIDTH="20"></TD>
{{end}}{{end}}
</TR>{{end}}
</TABLE>
`

var processorHTMLTemplate = template.Must(template.New("processor").Parse(processorHTML))
