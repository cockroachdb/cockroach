// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfrapb

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
)

// DiagramFlags contains diagram settings.
type DiagramFlags struct {
	// ShowInputTypes adds input type information.
	ShowInputTypes bool

	// MakeDeterministic resets all stats that can vary from run to run (like
	// execution time), suitable for tests. See CompositeStats.MakeDeterministic.
	MakeDeterministic bool
}

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
func (f *FiltererSpec) summary() (string, []string) {
	return "Filterer", []string{
		fmt.Sprintf("Filter: %s", f.Filter),
	}
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
func (v *VectorSearchSpec) summary() (string, []string) {
	details := []string{
		fmt.Sprintf("%s@%s", v.FetchSpec.TableName, v.FetchSpec.IndexName),
		fmt.Sprintf("Nearest Neighbor Target Count: %d", v.TargetNeighborCount),
		fmt.Sprintf("Query Vector: %s", vector.T(v.QueryVector).String()),
	}
	if len(v.PrefixKey) > 0 {
		vals, _ := encoding.PrettyPrintValuesWithTypes(nil /* valDirs */, v.PrefixKey)
		details = append(details, fmt.Sprintf("Prefix Vals: %s", strings.Join(vals, "/")))
	}
	return "VectorSearch", details
}

// summary implements the diagramCellType interface.
func (v *VectorMutationSearchSpec) summary() (string, []string) {
	var mutationType string
	if v.IsIndexPut {
		mutationType = "Index Put"
	} else {
		mutationType = "Index Delete"
	}
	details := []string{
		fmt.Sprintf("%s@%s", v.FetchSpec.TableName, v.FetchSpec.IndexName),
		mutationType,
		fmt.Sprintf("Query Vector Col: @%d", v.QueryVectorColumnOrdinal+1),
	}
	if len(v.PrefixKeyColumnOrdinals) > 0 {
		details = append(details,
			fmt.Sprintf("Prefix Columns: %s", colListStr(v.PrefixKeyColumnOrdinals)),
		)
	}
	return "VectorMutationSearch", details
}

// summary implements the diagramCellType interface.
func (a *AggregatorSpec) summary() (string, []string) {
	details := make([]string, 0, len(a.Aggregations)+1)
	if len(a.GroupCols) > 0 {
		details = append(details, colListStr(a.GroupCols))
	}
	if len(a.OrderedGroupCols) > 0 {
		details = append(details, fmt.Sprintf("Ordered: %s", colListStr(a.OrderedGroupCols)))
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

func appendColumns(details []string, columns []fetchpb.IndexFetchSpec_Column) []string {
	var b strings.Builder
	b.WriteString("Columns:")
	const wrapAt = 100
	for i := range columns {
		if i > 0 {
			b.WriteByte(',')
		}
		name := columns[i].Name
		if b.Len()+len(name)+1 > wrapAt {
			details = append(details, b.String())
			b.Reset()
		}
		b.WriteByte(' ')
		b.WriteString(name)
	}
	details = append(details, b.String())
	return details
}

// summary implements the diagramCellType interface.
func (tr *TableReaderSpec) summary() (string, []string) {
	details := make([]string, 0, 3)
	details = append(details, fmt.Sprintf("%s@%s", tr.FetchSpec.TableName, tr.FetchSpec.IndexName))
	details = appendColumns(details, tr.FetchSpec.FetchedColumns)

	if len(tr.Spans) > 0 {
		// only show the first span
		keyDirs := make([]encoding.Direction, len(tr.FetchSpec.KeyAndSuffixColumns))
		for i := range keyDirs {
			keyDirs[i] = encoding.Ascending
			if tr.FetchSpec.KeyAndSuffixColumns[i].Direction == catenumpb.IndexColumn_DESC {
				keyDirs[i] = encoding.Descending
			}
		}

		var spanStr strings.Builder
		spanStr.WriteString("Spans: ")
		spanStr.WriteString(catalogkeys.PrettySpan(keyDirs, tr.Spans[0], 2))

		if len(tr.Spans) > 1 {
			spanStr.WriteString(fmt.Sprintf(" and %d other", len(tr.Spans)-1))
		}

		if len(tr.Spans) > 2 {
			spanStr.WriteString("s") // pluralize the 'other'
		}

		details = append(details, spanStr.String())
	}

	if tr.MaxTimestampAgeNanos != 0 {
		details = append(details, fmt.Sprintf(
			"Inconsistent scan (max ts age %s)", time.Duration(tr.MaxTimestampAgeNanos),
		))
	}

	return "TableReader", details
}

// summary implements the diagramCellType interface.
func (jr *JoinReaderSpec) summary() (string, []string) {
	details := make([]string, 0, 5)
	if jr.Type != descpb.InnerJoin {
		details = append(details, joinTypeDetail(jr.Type))
	}
	details = append(details, fmt.Sprintf("%s@%s", jr.FetchSpec.TableName, jr.FetchSpec.IndexName))
	if len(jr.LookupColumns) > 0 {
		details = append(details, fmt.Sprintf("Lookup join on: %s", colListStr(jr.LookupColumns)))
	}
	if !jr.LookupExpr.Empty() {
		details = append(details, fmt.Sprintf("Lookup join on: %s", jr.LookupExpr))
	}
	if !jr.RemoteLookupExpr.Empty() {
		details = append(details, fmt.Sprintf("Remote lookup join on: %s", jr.RemoteLookupExpr))
	}
	if !jr.OnExpr.Empty() {
		details = append(details, fmt.Sprintf("ON %s", jr.OnExpr))
	}
	if jr.LeftJoinWithPairedJoiner {
		details = append(details, "second join in paired-join")
	}
	if jr.OutputGroupContinuationForLeftRow {
		details = append(details, "first join in paired-join")
	}
	details = appendColumns(details, jr.FetchSpec.FetchedColumns)
	return "JoinReader", details
}

func joinTypeDetail(joinType descpb.JoinType) string {
	typeStr := strings.Replace(joinType.String(), "_", " ", -1)
	if joinType == descpb.IntersectAllJoin || joinType == descpb.ExceptAllJoin {
		return fmt.Sprintf("Type: %s", typeStr)
	}
	return fmt.Sprintf("Type: %s JOIN", typeStr)
}

// summary implements the diagramCellType interface.
func (hj *HashJoinerSpec) summary() (string, []string) {
	name := "HashJoiner"
	if hj.Type.IsSetOpJoin() {
		name = "HashSetOp"
	}

	details := make([]string, 0, 4)

	if hj.Type != descpb.InnerJoin {
		details = append(details, joinTypeDetail(hj.Type))
	}
	if len(hj.LeftEqColumns) > 0 {
		details = append(details, fmt.Sprintf(
			"left(%s)=right(%s)",
			colListStr(hj.LeftEqColumns), colListStr(hj.RightEqColumns),
		))
	}
	if !hj.OnExpr.Empty() {
		details = append(details, fmt.Sprintf("ON %s", hj.OnExpr))
	}

	return name, details
}

// summary implements the diagramCellType interface.
func (ifs *InvertedFiltererSpec) summary() (string, []string) {
	name := "InvertedFilterer"
	var b strings.Builder
	for i := range ifs.InvertedExpr.SpansToRead {
		if i > 0 {
			fmt.Fprintf(&b, " and %d others", len(ifs.InvertedExpr.SpansToRead)-1)
			break
		} else {
			fmt.Fprintf(&b, "%s", ifs.InvertedExpr.SpansToRead[i].String())
		}
	}
	details := append([]string(nil), fmt.Sprintf(
		"InvertedExpr on @%d: spans %s", ifs.InvertedColIdx, b.String()))
	return name, details
}

func orderedJoinDetails(
	joinType descpb.JoinType, left, right Ordering, onExpr Expression,
) []string {
	details := make([]string, 0, 3)

	if joinType != descpb.InnerJoin {
		details = append(details, joinTypeDetail(joinType))
	}
	details = append(details, fmt.Sprintf(
		"left(%s)=right(%s)", left.diagramString(), right.diagramString(),
	))

	if !onExpr.Empty() {
		details = append(details, fmt.Sprintf("ON %s", onExpr))
	}

	return details
}

// summary implements the diagramCellType interface.
func (mj *MergeJoinerSpec) summary() (string, []string) {
	name := "MergeJoiner"
	if mj.Type.IsSetOpJoin() {
		name = "MergeSetOp"
	}
	return name, orderedJoinDetails(mj.Type, mj.LeftOrdering, mj.RightOrdering, mj.OnExpr)
}

// summary implements the diagramCellType interface.
func (zj *ZigzagJoinerSpec) summary() (string, []string) {
	name := "ZigzagJoiner"
	details := make([]string, 0, len(zj.Sides)+1)
	for i := range zj.Sides {
		fetchSpec := &zj.Sides[i].FetchSpec
		details = append(details, fmt.Sprintf(
			"Side %d: %s@%s", i, fetchSpec.TableName, fetchSpec.IndexName,
		))
		details = appendColumns(details, fetchSpec.FetchedColumns)
	}
	if !zj.OnExpr.Empty() {
		details = append(details, fmt.Sprintf("ON %s", zj.OnExpr))
	}
	return name, details
}

// summary implements the diagramCellType interface.
func (ij *InvertedJoinerSpec) summary() (string, []string) {
	details := make([]string, 0, 5)
	if ij.Type != descpb.InnerJoin {
		details = append(details, joinTypeDetail(ij.Type))
	}
	details = append(details, fmt.Sprintf("%s@%s", ij.FetchSpec.TableName, ij.FetchSpec.IndexName))
	details = append(details, fmt.Sprintf("InvertedExpr %s", ij.InvertedExpr))
	if !ij.OnExpr.Empty() {
		details = append(details, fmt.Sprintf("ON %s", ij.OnExpr))
	}
	if ij.OutputGroupContinuationForLeftRow {
		details = append(details, "first join in paired-join")
	}
	details = appendColumns(details, ij.FetchSpec.FetchedColumns)
	return "InvertedJoiner", details
}

// summary implements the diagramCellType interface.
func (s *SorterSpec) summary() (string, []string) {
	details := []string{s.OutputOrdering.diagramString()}
	if s.OrderingMatchLen != 0 {
		details = append(details, fmt.Sprintf("match len: %d", s.OrderingMatchLen))
	}
	if s.Limit > 0 {
		details = append(details, fmt.Sprintf("TopK: %d", s.Limit))
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
func (m *BackupDataSpec) summary() (string, []string) {
	var spanStr strings.Builder
	if len(m.Spans) > 0 {
		spanStr.WriteString(fmt.Sprintf("Spans [%d]: ", len(m.Spans)+len(m.IntroducedSpans)))
		const limit = 3
		for i := 0; i < len(m.Spans) && i < limit; i++ {
			if i > 0 {
				spanStr.WriteString(", ")
			}
			spanStr.WriteString(m.Spans[i].String())
		}
		if len(m.Spans) > limit {
			spanStr.WriteString("...")
		}
	}

	details := []string{
		spanStr.String(),
	}
	return "BACKUP", details
}

// summary implements the diagramCellType interface.
func (d *DistinctSpec) summary() (string, []string) {
	details := []string{
		colListStr(d.DistinctColumns),
	}
	if len(d.OrderedColumns) > 0 {
		details = append(details, fmt.Sprintf("Ordered: %s", colListStr(d.OrderedColumns)))
	}
	return "Distinct", details
}

// summary implements the diagramCellType interface.
func (o *OrdinalitySpec) summary() (string, []string) {
	return "Ordinality", []string{}
}

// summary implements the diagramCellType interface.
func (d *ProjectSetSpec) summary() (string, []string) {
	var details []string
	for _, expr := range d.Exprs {
		details = append(details, expr.String())
	}
	return "ProjectSet", details
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

func (is *InputSyncSpec) summary(showTypes bool) (string, []string) {
	typs := make([]string, 0, len(is.ColumnTypes)+1)
	if showTypes {
		for _, typ := range is.ColumnTypes {
			typs = append(typs, typ.Name())
		}
	}
	switch is.Type {
	case InputSyncSpec_PARALLEL_UNORDERED:
		return "unordered", typs
	case InputSyncSpec_ORDERED:
		return "ordered", append(typs, is.Ordering.diagramString())
	case InputSyncSpec_SERIAL_UNORDERED:
		return "serial unordered", typs
	default:
		return "unknown", []string{}
	}
}

// summary implements the diagramCellType interface.
func (r *LocalPlanNodeSpec) summary() (string, []string) {
	return fmt.Sprintf("local %s %d", r.Name, r.RowSourceIdx), []string{}
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
	var res []string
	if post.Projection {
		outputColumns := "None"
		if len(post.OutputColumns) > 0 {
			outputColumns = colListStr(post.OutputColumns)
		}
		res = append(res, fmt.Sprintf("Out: %s", outputColumns))
	}
	if len(post.RenderExprs) > 0 {
		var buf bytes.Buffer
		buf.WriteString("Render: ")
		for i, expr := range post.RenderExprs {
			if i > 0 {
				buf.WriteString(", ")
			}
			// Remove any spaces in the expression (makes things more compact
			// and it's easier to visually separate expressions).
			buf.WriteString(strings.Replace(expr.String(), " ", "", -1))
		}
		res = append(res, buf.String())
	}
	if post.Limit != 0 || post.Offset != 0 {
		var buf bytes.Buffer
		if post.Limit != 0 {
			fmt.Fprintf(&buf, "Limit %d", post.Limit)
		}
		if post.Offset != 0 {
			if buf.Len() != 0 {
				buf.WriteByte(' ')
			}
			fmt.Fprintf(&buf, "Offset %d", post.Offset)
		}
		res = append(res, buf.String())
	}
	return res
}

// summary implements the diagramCellType interface.
func (c *RestoreDataSpec) summary() (string, []string) {
	return "RestoreDataSpec", []string{}
}

// summary implements the diagramCellType interface.
func (c *CloudStorageTestSpec) summary() (string, []string) {
	return "CloudStorageTestSpec", []string{}
}

// summary implements the diagramCellType interface.
func (c *ReadImportDataSpec) summary() (string, []string) {
	ss := make([]string, 0, len(c.Uri))
	for _, s := range c.Uri {
		ss = append(ss, s)
	}
	return "ReadImportData", ss
}

// summary implements the diagramCellType interface.
func (s *StreamIngestionDataSpec) summary() (string, []string) {
	const (
		specLimit = 3
		spanLimit = 3
	)

	annotations := []string{
		"Partitions:",
	}

	// Sort partitions by ID for stable output.
	srcIDs := make([]string, 0, len(s.PartitionSpecs))
	for k := range s.PartitionSpecs {
		srcIDs = append(srcIDs, k)
	}
	sort.Strings(srcIDs)

	specCount := 0
	for _, srcID := range srcIDs {
		specCount++
		if specCount > specLimit {
			annotations = append(annotations, fmt.Sprintf("and %d more partitions", len(s.PartitionSpecs)-specLimit))
			break
		}
		p := s.PartitionSpecs[srcID]

		annotations = append(annotations, fmt.Sprintf("Source node %s, spans:", srcID))
		for i, span := range p.Spans {
			if i == spanLimit {
				annotations = append(annotations, fmt.Sprintf("and %d more spans", len(p.Spans)-spanLimit))
				break
			}
			annotations = append(annotations, fmt.Sprintf("%v", span))
		}
	}

	return "StreamIngestionData", annotations
}

func (s *LogicalReplicationWriterSpec) summary() (string, []string) {
	const spanLimit = 9

	tableNames := []string{}
	for _, table := range s.TableMetadataByDestID {
		tableNames = append(tableNames, table.SourceDescriptor.Name)
	}

	annotations := []string{
		fmt.Sprintf("Tables: %s", strings.Join(tableNames, ",")),
		fmt.Sprintf("Source node %s", s.PartitionSpec.SrcInstanceID),
		"Spans:",
	}

	for i, span := range s.PartitionSpec.Spans {
		if i == spanLimit {
			annotations = append(annotations, fmt.Sprintf("and %d more spans", len(s.PartitionSpec.Spans)-spanLimit))
			break
		}
		annotations = append(annotations, fmt.Sprintf("%v", span))
	}

	return "LogicalReplicationWriter", annotations
}

func (s *LogicalReplicationOfflineScanSpec) summary() (string, []string) {
	const spanLimit = 9

	srcTableIDs := []string{}
	for _, pair := range s.Rekey {
		srcTableIDs = append(srcTableIDs, fmt.Sprintf("%d", pair.OldID))
	}

	annotations := []string{
		fmt.Sprintf("Src Table IDs: %s", strings.Join(srcTableIDs, ",")),
		fmt.Sprintf("Source node %s", s.PartitionSpec.SrcInstanceID),
		"Spans:",
	}

	for i, span := range s.PartitionSpec.Spans {
		if i == spanLimit {
			annotations = append(annotations, fmt.Sprintf("and %d more spans", len(s.PartitionSpec.Spans)-spanLimit))
			break
		}
		annotations = append(annotations, fmt.Sprintf("%v", span))
	}

	return "LogicalReplicationOfflineScanWriter", annotations
}

// summary implements the diagramCellType interface.
func (s *StreamIngestionFrontierSpec) summary() (string, []string) {
	annotations := []string{
		fmt.Sprintf("streamID: %d", s.StreamID),
	}
	return "StreamIngestionFrontier", annotations
}

// summary implements the diagramCellType interface.
func (s *IndexBackfillMergerSpec) summary() (string, []string) {
	return "IndexBackfillMerger", []string{}
}

// summary implements the diagramCellType interface.
func (s *ExportSpec) summary() (string, []string) {
	return "Exporter", []string{s.Destination}
}

// summary implements the diagramCellType interface.
func (s *BulkRowWriterSpec) summary() (string, []string) {
	return "BulkRowWriterSpec", []string{}
}

// summary implements the diagramCellType interface.
func (w *WindowerSpec) summary() (string, []string) {
	details := make([]string, 0, len(w.WindowFns))
	if len(w.PartitionBy) > 0 {
		details = append(details, fmt.Sprintf("PARTITION BY: %s", colListStr(w.PartitionBy)))
	}
	for _, windowFn := range w.WindowFns {
		var buf bytes.Buffer
		if windowFn.Func.WindowFunc != nil {
			buf.WriteString(windowFn.Func.WindowFunc.String())
		} else {
			buf.WriteString(windowFn.Func.AggregateFunc.String())
		}
		buf.WriteByte('(')
		buf.WriteString(colListStr(windowFn.ArgsIdxs))
		buf.WriteByte(')')
		if len(windowFn.Ordering.Columns) > 0 {
			buf.WriteString(" (ORDER BY ")
			buf.WriteString(windowFn.Ordering.diagramString())
			buf.WriteByte(')')
		}
		details = append(details, buf.String())
	}

	return "Windower", details
}

// summary implements the diagramCellType interface.
func (s *ChangeAggregatorSpec) summary() (string, []string) {
	var spanStr strings.Builder
	if len(s.Watches) > 0 {
		spanStr.WriteString(fmt.Sprintf("Watches [%d]: ", len(s.Watches)))
		const limit = 3
		for i := 0; i < len(s.Watches) && i < limit; i++ {
			if i > 0 {
				spanStr.WriteString(", ")
			}
			spanStr.WriteString(s.Watches[i].Span.String())
		}
		if len(s.Watches) > limit {
			spanStr.WriteString("...")
		}
	}

	details := []string{
		spanStr.String(),
	}
	return "ChangeAggregator", details
}

// summary implements the diagramCellType interface.
func (s *ChangeFrontierSpec) summary() (string, []string) {
	return "ChangeFrontier", []string{}
}

// summary implements the diagramCellType interface.
func (s *TTLSpec) summary() (string, []string) {
	details := s.RowLevelTTLDetails
	return "TTL", []string{
		fmt.Sprintf("JobID: %d", s.JobID),
		fmt.Sprintf("TableID: %d", details.TableID),
		fmt.Sprintf("TableVersion: %d", details.TableVersion),
	}
}

// summary implements the diagramCellType interface.
func (s *HashGroupJoinerSpec) summary() (string, []string) {
	_, details := s.HashJoinerSpec.summary()
	if len(s.JoinOutputColumns) > 0 {
		details = append(details, "Join Projection: "+colListStr(s.JoinOutputColumns))
	}
	_, aggDetails := s.AggregatorSpec.summary()
	if len(s.AggregatorSpec.GroupCols) > 0 {
		// For hash group-join the equality columns of the join are always the
		// same as the grouping columns of the aggregations, so we remove this
		// duplicated information (which is included as the first line in the
		// summary of the aggregation spec).
		aggDetails = aggDetails[1:]
	}
	details = append(details, aggDetails...)
	return "HashGroupJoiner", details
}

// summary implements the diagramCellType interface.
func (g *GenerativeSplitAndScatterSpec) summary() (string, []string) {
	detail := fmt.Sprintf("%d import spans, %d checkpointed spans", g.NumEntries, len(g.CheckpointedSpans))
	return "GenerativeSplitAndScatterSpec", []string{detail}
}

// summary implements the diagramCellType interface.
func (i *InsertSpec) summary() (string, []string) {
	return "Insert", []string{
		fmt.Sprintf("TableID: %d", i.Table.ID),
		fmt.Sprintf("AutoCommit: %t", i.AutoCommit),
	}
}

// summary implements the diagramCellType interface.
func (i *IngestStoppedSpec) summary() (string, []string) {
	detail := fmt.Sprintf("job %d ingest stopped spans", i.JobID)
	return "IngestStoppedSpec", []string{detail}
}

type diagramCell struct {
	Title   string   `json:"title"`
	Details []string `json:"details"`
}

type diagramProcessor struct {
	NodeIdx     int           `json:"nodeIdx"`
	Inputs      []diagramCell `json:"inputs"`
	Core        diagramCell   `json:"core"`
	Outputs     []diagramCell `json:"outputs"`
	StageID     int32         `json:"stage"`
	ProcessorID int32         `json:"processorID"`
}

type diagramEdge struct {
	SourceProc   int      `json:"sourceProc"`
	SourceOutput int      `json:"sourceOutput"`
	DestProc     int      `json:"destProc"`
	DestInput    int      `json:"destInput"`
	Stats        []string `json:"stats,omitempty"`
	StreamID     StreamID `json:"streamID"`
}

// FlowDiagram is a plan diagram that can be made into a URL.
type FlowDiagram interface {
	// ToURL generates the json data for a flow diagram and a URL which encodes the
	// diagram.
	ToURL() (string, url.URL, error)

	// AddSpans adds stats extracted from the input spans to the diagram.
	AddSpans([]tracingpb.RecordedSpan)

	// UpdateComponentFractionProgressed updates the per-component progress on the
	// diagram.
	UpdateComponentFractionProgressed(perComponentProgress map[ComponentID]float32)
}

type diagramData struct {
	SQL        string             `json:"sql"`
	NodeNames  []string           `json:"nodeNames"`
	Processors []diagramProcessor `json:"processors"`
	Edges      []diagramEdge      `json:"edges"`
	FlowID     FlowID             `json:"flow_id"`
	Flags      DiagramFlags       `json:"flags"`

	sqlInstanceIDs []base.SQLInstanceID
}

var _ FlowDiagram = &diagramData{}

// FromURL converts a FlowDiagram URL to a FlowDiagram.
func FromURL(url string) (FlowDiagram, error) {
	r, err := decodeURLToJSON(url)
	if err != nil {
		return &diagramData{}, errors.Wrap(err, "failed to decode URL to JSON")
	}

	d := diagramData{}
	err = json.NewDecoder(&r).Decode(&d)
	for _, name := range d.NodeNames {
		sqlInstanceID, err := strconv.Atoi(name)
		if err != nil {
			return nil, err
		}
		d.sqlInstanceIDs = append(d.sqlInstanceIDs, base.SQLInstanceID(sqlInstanceID))
	}
	return &d, err
}

// ToURL implements the FlowDiagram interface.
func (d diagramData) ToURL() (string, url.URL, error) {
	if d.Flags.MakeDeterministic {
		d.FlowID = FlowID{uuid.Nil}
		for _, p := range d.Processors {
			p.ProcessorID = 0
		}
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(d); err != nil {
		return "", url.URL{}, err
	}
	return encodeJSONToURL(buf)
}

// UpdateComponentFractionProgressed implements the FlowDiagram interface.
func (d *diagramData) UpdateComponentFractionProgressed(
	perComponentProgress map[ComponentID]float32,
) {
	for i := range d.Processors {
		p := &d.Processors[i]
		sqlInstanceID := d.sqlInstanceIDs[p.NodeIdx]
		component := ProcessorComponentID(sqlInstanceID, d.FlowID, p.ProcessorID)
		if fraction, ok := perComponentProgress[component]; ok {
			var updated bool
			for i, detail := range p.Core.Details {
				if strings.HasPrefix(detail, "progress") {
					p.Core.Details[i] = fmt.Sprintf("progress: %.2f", fraction)
					updated = true
					break
				}
			}

			// If this is the first time we are recording the component progress then
			// we simply append the new details.
			//
			// TODO(adityamaru): Consider making p.Core.Details a map instead of a
			// slice since all the values stored in the slice are key-values.
			if !updated {
				p.Core.Details = append(p.Core.Details, fmt.Sprintf("progress: %.2f", fraction))
			}
		}
	}
}

// AddSpans implements the FlowDiagram interface.
func (d *diagramData) AddSpans(spans []tracingpb.RecordedSpan) {
	statsMap := ExtractStatsFromSpans(spans, d.Flags.MakeDeterministic)
	for i := range d.Processors {
		p := &d.Processors[i]
		sqlInstanceID := d.sqlInstanceIDs[p.NodeIdx]
		component := ProcessorComponentID(sqlInstanceID, d.FlowID, p.ProcessorID)
		if compStats := statsMap[component]; compStats != nil {
			p.Core.Details = append(p.Core.Details, compStats.StatsForQueryPlan()...)
		}
	}
	for i := range d.Edges {
		originSQLInstanceID := d.sqlInstanceIDs[d.Processors[d.Edges[i].SourceProc].NodeIdx]
		component := StreamComponentID(originSQLInstanceID, d.FlowID, d.Edges[i].StreamID)
		if compStats := statsMap[component]; compStats != nil {
			d.Edges[i].Stats = compStats.StatsForQueryPlan()
		}
	}
}

// generateDiagramData generates the diagram data, given a list of flows (one
// per node). The sqlInstanceIDs list corresponds 1-1 to the flows list.
func generateDiagramData(
	sql string, flows []FlowSpec, sqlInstanceIDs []base.SQLInstanceID, flags DiagramFlags,
) (FlowDiagram, error) {
	d := &diagramData{
		SQL:            sql,
		sqlInstanceIDs: sqlInstanceIDs,
		Flags:          flags,
	}
	d.NodeNames = make([]string, len(sqlInstanceIDs))
	for i := range d.NodeNames {
		d.NodeNames[i] = sqlInstanceIDs[i].String()
	}

	if len(flows) > 0 {
		d.FlowID = flows[0].FlowID
		for i := 1; i < len(flows); i++ {
			if flows[i].FlowID != d.FlowID {
				return nil, errors.AssertionFailedf("flow ID mismatch within a diagram")
			}
		}
	}

	// inPorts maps streams to their "destination" attachment point. Only DestProc
	// and DestInput are set in each diagramEdge value.
	inPorts := make(map[StreamID]diagramEdge)
	syncResponseNode := -1

	pIdx := 0
	for n := range flows {
		for _, p := range flows[n].Processors {
			proc := diagramProcessor{NodeIdx: n}
			proc.Core.Title, proc.Core.Details = p.Core.GetValue().(diagramCellType).summary()
			proc.Core.Title += fmt.Sprintf("/%d", p.ProcessorID)
			proc.ProcessorID = p.ProcessorID
			proc.Core.Details = append(proc.Core.Details, p.Post.summary()...)

			// We need explicit synchronizers if we have multiple inputs, or if the
			// one input has multiple input streams.
			if len(p.Input) > 1 || (len(p.Input) == 1 && len(p.Input[0].Streams) > 1) {
				proc.Inputs = make([]diagramCell, len(p.Input))
				for i, s := range p.Input {
					proc.Inputs[i].Title, proc.Inputs[i].Details = s.summary(flags.ShowInputTypes)
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
							return nil, errors.Errorf("multiple nodes with SyncResponse")
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
			// When generating stats, spans are mapped from processor ID in the span
			// tags to processor ID in the diagram data. To avoid clashing with
			// the processor with ID 0, assign an impossible processorID.
			ProcessorID: -1,
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
						StreamID:     o.StreamID,
					}
					if o.Type == StreamEndpointSpec_SYNC_RESPONSE {
						edge.DestProc = len(d.Processors) - 1
					} else {
						to, ok := inPorts[o.StreamID]
						if !ok {
							return nil, errors.Errorf("stream %d has no destination", o.StreamID)
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

// GeneratePlanDiagram generates the data for a flow diagram. There should be
// one FlowSpec per node. The function assumes that StreamIDs are unique across
// all flows.
func GeneratePlanDiagram(
	sql string, flows map[base.SQLInstanceID]*FlowSpec, flags DiagramFlags,
) (FlowDiagram, error) {
	// We sort the flows by node because we want the diagram data to be
	// deterministic.
	sqlInstanceIDs := make([]base.SQLInstanceID, 0, len(flows))
	for n := range flows {
		sqlInstanceIDs = append(sqlInstanceIDs, n)
	}
	sort.Slice(sqlInstanceIDs, func(i, j int) bool {
		return sqlInstanceIDs[i] < sqlInstanceIDs[j]
	})

	flowSlice := make([]FlowSpec, len(sqlInstanceIDs))
	for i, n := range sqlInstanceIDs {
		flowSlice[i] = *flows[n]
	}

	return generateDiagramData(sql, flowSlice, sqlInstanceIDs, flags)
}

// GeneratePlanDiagramURL generates the json data for a flow diagram and a
// URL which encodes the diagram. There should be one FlowSpec per node. The
// function assumes that StreamIDs are unique across all flows.
func GeneratePlanDiagramURL(
	sql string, flows map[base.SQLInstanceID]*FlowSpec, flags DiagramFlags,
) (string, url.URL, error) {
	d, err := GeneratePlanDiagram(sql, flows, flags)
	if err != nil {
		return "", url.URL{}, err
	}
	return d.ToURL()
}

func decodeURLToJSON(urlStr string) (bytes.Buffer, error) {
	var json bytes.Buffer
	u, err := url.Parse(urlStr)
	if err != nil {
		return json, errors.Wrap(err, "failed to Parse URL")
	}

	compressed := u.Fragment
	decoder := base64.NewDecoder(base64.URLEncoding, bytes.NewReader([]byte(compressed)))
	decompressor, err := zlib.NewReader(decoder)
	if err != nil {
		return json, errors.Wrap(err, "failed in NewReader")
	}
	b, err := io.ReadAll(decompressor)
	if err != nil {
		return json, errors.Wrap(err, "failed in ReadAll")
	}
	if err := decompressor.Close(); err != nil {
		return json, err
	}

	_, err = json.Write(b)
	return json, err
}

func encodeJSONToURL(json bytes.Buffer) (string, url.URL, error) {
	var compressed bytes.Buffer
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
		Fragment: compressed.String(),
	}
	return jsonStr, url, nil
}
