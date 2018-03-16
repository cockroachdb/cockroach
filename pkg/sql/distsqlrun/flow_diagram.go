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

package distsqlrun

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	humanize "github.com/dustin/go-humanize"
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
		fmt.Fprintf(&buf, "@%d", c.ColIdx+1)
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
		fmt.Fprintf(&buf, "@%d", c+1)
	}
	return buf.String()
}

// summary implements the diagramCellType interface.
func (*NoopCoreSpec) summary() (string, []string) {
	return "No-op", []string{}
}

// summary implements the diagramCellType interface.
func (mts *MetadataTestSenderSpec) summary() (string, []string) {
	return "MetadataTestSender", []string{mts.ID}
}

// summary implements the diagramCellType interface.
func (*MetadataTestReceiverSpec) summary() (string, []string) {
	return "MetadataTestReceiver", []string{}
}

// summary implements the diagramCellType interface.
func (v *ValuesCoreSpec) summary() (string, []string) {
	var bytes uint64
	for _, b := range v.RawBytes {
		bytes += uint64(len(b))
	}
	detail := fmt.Sprintf("%s (%d chunks)", humanize.IBytes(bytes), len(v.RawBytes))
	return "Values", []string{detail}
}

// summary implements the diagramCellType interface.
func (a *AggregatorSpec) summary() (string, []string) {
	details := make([]string, 0, len(a.Aggregations)+1)
	if len(a.GroupCols) > 0 {
		details = append(details, colListStr(a.GroupCols))
	}
	for _, agg := range a.Aggregations {
		var buf bytes.Buffer
		buf.WriteString(agg.Func.String())
		buf.WriteByte('(')

		if agg.Distinct {
			buf.WriteString("DISTINCT ")
		}
		buf.WriteString(colListStr(agg.ColIdx))
		buf.WriteByte(')')
		if agg.FilterColIdx != nil {
			fmt.Fprintf(&buf, " FILTER @%d", *agg.FilterColIdx+1)
		}

		details = append(details, buf.String())
	}

	return "Aggregator", details
}

func indexDetails(indexIdx uint32, desc *sqlbase.TableDescriptor) []string {
	index := "primary"
	if indexIdx > 0 {
		index = desc.Indexes[indexIdx-1].Name
	}
	return []string{fmt.Sprintf("%s@%s", index, desc.Name)}
}

// summary implements the diagramCellType interface.
func (tr *TableReaderSpec) summary() (string, []string) {
	// TODO(radu): a summary of the spans
	return "TableReader", indexDetails(tr.IndexIdx, &tr.Table)
}

// summary implements the diagramCellType interface.
func (jr *JoinReaderSpec) summary() (string, []string) {
	index := "primary"
	if jr.IndexIdx > 0 {
		index = jr.Table.Indexes[jr.IndexIdx-1].Name
	}
	details := make([]string, 0, 2)
	details = append(details, fmt.Sprintf("%s@%s", index, jr.Table.Name))
	if jr.LookupColumns != nil {
		details = append(details, fmt.Sprintf("Lookup join on: %s", colListStr(jr.LookupColumns)))
	}
	return "JoinReader", details
}

func joinTypeDetail(joinType sqlbase.JoinType) string {
	typeStr := strings.Replace(joinType.String(), "_", " ", -1)
	if joinType == sqlbase.IntersectAllJoin || joinType == sqlbase.ExceptAllJoin {
		return fmt.Sprintf("Type: %s", typeStr)
	}
	return fmt.Sprintf("Type: %s JOIN", typeStr)
}

// summary implements the diagramCellType interface.
func (hj *HashJoinerSpec) summary() (string, []string) {
	name := "HashJoiner"
	if isSetOpJoin(hj.Type) {
		name = "HashSetOp"
	}

	details := make([]string, 0, 4)

	if hj.Type != sqlbase.InnerJoin {
		details = append(details, joinTypeDetail(hj.Type))
	}
	if len(hj.LeftEqColumns) > 0 {
		details = append(details, fmt.Sprintf(
			"left(%s)=right(%s)",
			colListStr(hj.LeftEqColumns), colListStr(hj.RightEqColumns),
		))
	}
	if hj.OnExpr.Expr != "" {
		details = append(details, fmt.Sprintf("ON %s", hj.OnExpr.Expr))
	}
	if hj.MergedColumns {
		details = append(details, fmt.Sprintf("Merged columns: %d", len(hj.LeftEqColumns)))
	}

	return name, details
}

func orderedJoinDetails(
	joinType sqlbase.JoinType, left, right Ordering, onExpr Expression,
) []string {
	details := make([]string, 0, 3)

	if joinType != sqlbase.InnerJoin {
		details = append(details, joinTypeDetail(joinType))
	}
	details = append(details, fmt.Sprintf(
		"left(%s)=right(%s)", left.diagramString(), right.diagramString(),
	))

	if onExpr.Expr != "" {
		details = append(details, fmt.Sprintf("ON %s", onExpr.Expr))
	}

	return details
}

// summary implements the diagramCellType interface.
func (mj *MergeJoinerSpec) summary() (string, []string) {
	name := "MergeJoiner"
	if isSetOpJoin(mj.Type) {
		name = "MergeSetOp"
	}
	return name, orderedJoinDetails(mj.Type, mj.LeftOrdering, mj.RightOrdering, mj.OnExpr)
}

// summary implements the diagramCellType interface.
func (irj *InterleavedReaderJoinerSpec) summary() (string, []string) {
	// As of right now, we only plan InterleaveReaderJoiner with two
	// tables.
	tables := irj.Tables[:2]
	details := make([]string, 0, len(tables)*6+3)
	for i, table := range tables {
		// left or right label
		var tableLabel string
		if i == 0 {
			tableLabel = "Left"
		} else if i == 1 {
			tableLabel = "Right"
		}
		details = append(details, tableLabel)
		// table@index name
		details = append(details, indexDetails(table.IndexIdx, &table.Desc)...)
		// Post process (filters, projections, renderExprs, limits/offsets)
		details = append(details, table.Post.summaryWithPrefix(fmt.Sprintf("%s ", tableLabel))...)
	}
	details = append(details, "Joiner")
	details = append(
		details, orderedJoinDetails(irj.Type, tables[0].Ordering, tables[1].Ordering, irj.OnExpr)...,
	)
	return "InterleaveReaderJoiner", details
}

// summary implements the diagramCellType interface.
func (s *SorterSpec) summary() (string, []string) {
	details := []string{s.OutputOrdering.diagramString()}
	if s.OrderingMatchLen != 0 {
		details = append(details, fmt.Sprintf("match len: %d", s.OrderingMatchLen))
	}
	return "Sorter", details
}

// summary implements the diagramCellType interface.
func (bf *BackfillerSpec) summary() (string, []string) {
	details := []string{
		bf.Table.Name,
		fmt.Sprintf("Type: %s", bf.Type.String()),
	}
	return "Backfiller", details
}

// summary implements the diagramCellType interface.
func (d *DistinctSpec) summary() (string, []string) {
	details := []string{
		colListStr(d.DistinctColumns),
	}
	if len(d.OrderedColumns) > 0 {
		details = append(details, fmt.Sprintf("Ordered: %s", colListStr(d.DistinctColumns)))
	}
	return "Distinct", details
}

// summary implements the diagramCellType interface.
func (s *SamplerSpec) summary() (string, []string) {
	details := []string{fmt.Sprintf("SampleSize: %d", s.SampleSize)}
	for _, sk := range s.Sketches {
		details = append(details, fmt.Sprintf("Stat: %s", colListStr(sk.Columns)))
	}

	return "Sampler", details
}

// summary implements the diagramCellType interface.
func (s *SampleAggregatorSpec) summary() (string, []string) {
	details := []string{
		fmt.Sprintf("SampleSize: %d", s.SampleSize),
	}
	for _, sk := range s.Sketches {
		s := fmt.Sprintf("Stat: %s", colListStr(sk.Columns))
		if sk.GenerateHistogram {
			s = fmt.Sprintf("%s (%d buckets)", s, sk.HistogramMaxBuckets)
		}
		details = append(details, s)
	}

	return "SampleAggregator", details
}

// summary implements the diagramCellType interface.
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

// summary implements the diagramCellType interface.
func (r *OutputRouterSpec) summary() (string, []string) {
	switch r.Type {
	case OutputRouterSpec_PASS_THROUGH:
		return "", []string{}
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

// summary implements the diagramCellType interface.
func (post *PostProcessSpec) summary() []string {
	return post.summaryWithPrefix("")
}

// prefix is prepended to every line outputted to disambiguate processors
// (namely InterleavedReaderJoiner) that have multiple PostProcessors.
func (post *PostProcessSpec) summaryWithPrefix(prefix string) []string {
	var res []string
	if post.Filter.Expr != "" {
		res = append(res, fmt.Sprintf("%sFilter: %s", prefix, post.Filter.Expr))
	}
	if post.Projection {
		outputColumns := "None"
		if len(post.OutputColumns) > 0 {
			outputColumns = colListStr(post.OutputColumns)
		}
		res = append(res, fmt.Sprintf("%sOut: %s", prefix, outputColumns))
	}
	if len(post.RenderExprs) > 0 {
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf("%sRender: ", prefix))
		for i, expr := range post.RenderExprs {
			if i > 0 {
				buf.WriteString(", ")
			}
			// Remove any spaces in the expression (makes things more compact
			// and it's easier to visually separate expressions).
			buf.WriteString(strings.Replace(expr.Expr, " ", "", -1))
		}
		res = append(res, buf.String())
	}
	if post.Limit != 0 || post.Offset != 0 {
		var buf bytes.Buffer
		if post.Limit != 0 {
			fmt.Fprintf(&buf, "%sLimit %d", prefix, post.Limit)
		}
		if post.Offset != 0 {
			if buf.Len() != 0 {
				buf.WriteByte(' ')
			}
			fmt.Fprintf(&buf, "%sOffset %d", prefix, post.Offset)
		}
		res = append(res, buf.String())
	}
	return res
}

// summary implements the diagramCellType interface.
func (c *ReadCSVSpec) summary() (string, []string) {
	ss := make([]string, 0, len(c.Uri))
	for _, s := range c.Uri {
		ss = append(ss, s)
	}
	return "ReadCSV", ss
}

// summary implements the diagramCellType interface.
func (s *SSTWriterSpec) summary() (string, []string) {
	var res []string
	for _, span := range s.Spans {
		res = append(res, fmt.Sprintf("%s: %s", span.Name, keys.PrettyPrint(nil, span.End)))
	}
	return "SSTWriter", res
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
	StageID int32         `json:"stage"`
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
			proc.Core.Details = append(proc.Core.Details, p.Post.summary()...)

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
			proc.StageID = p.StageID
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

// GeneratePlanDiagram generates the json data for a flow diagram.  There should // be one FlowSpec per node. The function assumes that StreamIDs are unique
// across all flows.
func GeneratePlanDiagram(flows map[roachpb.NodeID]FlowSpec, w io.Writer) error {
	// We sort the flows by node because we want the diagram data to be
	// deterministic.
	nodeIDs := make([]int, 0, len(flows))
	for n := range flows {
		nodeIDs = append(nodeIDs, int(n))
	}
	sort.Ints(nodeIDs)

	flowSlice := make([]FlowSpec, len(nodeIDs))
	nodeNames := make([]string, len(nodeIDs))
	for i, nVal := range nodeIDs {
		n := roachpb.NodeID(nVal)
		flowSlice[i] = flows[n]
		nodeNames[i] = n.String()
	}

	d, err := generateDiagramData(flowSlice, nodeNames)
	if err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(d)
}

// GeneratePlanDiagramWithURL generates the json data for a flow diagram and a
// URL which encodes the diagram. There should be one FlowSpec per node. The
// function assumes that StreamIDs are unique across all flows.
func GeneratePlanDiagramWithURL(flows map[roachpb.NodeID]FlowSpec) (string, url.URL, error) {
	var json, compressed bytes.Buffer
	if err := GeneratePlanDiagram(flows, &json); err != nil {
		return "", url.URL{}, err
	}
	jsonStr := json.String()

	encoder := base64.NewEncoder(base64.URLEncoding, &compressed)
	compressor := zlib.NewWriter(encoder)
	if _, err := json.WriteTo(compressor); err != nil {
		return "", url.URL{}, err
	}
	if err := compressor.Close(); err != nil {
		return "", url.URL{}, err
	}
	if err := encoder.Close(); err != nil {
		return "", url.URL{}, err
	}
	url := url.URL{
		Scheme:   "https",
		Host:     "cockroachdb.github.io",
		Path:     "distsqlplan/decode.html",
		RawQuery: compressed.String(),
	}

	return jsonStr, url, nil
}
