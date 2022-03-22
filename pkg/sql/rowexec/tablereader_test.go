// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/gogo/protobuf/types"
)

func TestTableReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a table where each row is:
	//
	//  |     a    |     b    |         sum         |         s           |
	//  |-----------------------------------------------------------------|
	//  | rowId/10 | rowId%10 | rowId/10 + rowId%10 | IntToEnglish(rowId) |

	aFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 10))
	}
	bFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % 10))
	}
	sumFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row/10 + row%10))
	}

	sqlutils.CreateTable(t, sqlDB, "t",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), INDEX bs (b,s)",
		19,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	td := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
	cols := td.PublicColumns()

	makeIndexSpan := func(start, end int) roachpb.Span {
		var span roachpb.Span
		prefix := roachpb.Key(rowenc.MakeIndexKeyPrefix(keys.SystemSQLCodec, td.GetID(), td.PublicNonPrimaryIndexes()[0].GetID()))
		span.Key = append(prefix, encoding.EncodeVarintAscending(nil, int64(start))...)
		span.EndKey = append(span.EndKey, prefix...)
		span.EndKey = append(span.EndKey, encoding.EncodeVarintAscending(nil, int64(end))...)
		return span
	}

	testCases := []struct {
		spec     execinfrapb.TableReaderSpec
		post     execinfrapb.PostProcessSpec
		expected string
	}{
		{
			spec: execinfrapb.TableReaderSpec{
				FetchSpec: makeFetchSpec(t, td, td.GetPrimaryIndex(), cols[0].GetID(), cols[1].GetID()),
				Spans:     []roachpb.Span{td.PrimaryIndexSpan(keys.SystemSQLCodec)},
			},
			expected: "[[0 1] [0 2] [0 3] [0 4] [0 5] [0 6] [0 7] [0 8] [0 9] [1 0] [1 1] [1 2] [1 3] [1 4] [1 5] [1 6] [1 7] [1 8] [1 9]]",
		},
		{
			spec: execinfrapb.TableReaderSpec{
				FetchSpec: makeFetchSpec(t, td, td.GetPrimaryIndex(), cols[3].GetID()),
				Spans:     []roachpb.Span{td.PrimaryIndexSpan(keys.SystemSQLCodec)},
			},
			post: execinfrapb.PostProcessSpec{
				Limit: 4,
			},
			expected: "[['one'] ['two'] ['three'] ['four']]",
		},
		{
			spec: execinfrapb.TableReaderSpec{
				FetchSpec: makeFetchSpec(t, td, td.ActiveIndexes()[1], cols[0].GetID(), cols[1].GetID()),
				Reverse:   true,
				Spans:     []roachpb.Span{makeIndexSpan(4, 6)},
				LimitHint: 1,
			},
			expected: "[[1 5] [0 5] [1 4] [0 4]]",
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "row-source", func(t *testing.T, rowSource bool) {
				ts := c.spec
				// Make a copy of Spans because the table reader will modify
				// them.
				ts.Spans = make([]roachpb.Span, len(c.spec.Spans))
				copy(ts.Spans, c.spec.Spans)

				st := s.ClusterSettings()
				evalCtx := tree.MakeTestingEvalContext(st)
				defer evalCtx.Stop(ctx)
				flowCtx := execinfra.FlowCtx{
					EvalCtx: &evalCtx,
					Cfg: &execinfra.ServerConfig{
						Settings: st,
						RangeCache: rangecache.NewRangeCache(s.ClusterSettings(), nil,
							func() int64 { return 2 << 10 }, s.Stopper(), s.TracerI().(*tracing.Tracer)),
					},
					Txn:    kv.NewTxn(ctx, s.DB(), s.NodeID()),
					NodeID: evalCtx.NodeID,
				}

				var out execinfra.RowReceiver
				var buf *distsqlutils.RowBuffer
				if !rowSource {
					buf = &distsqlutils.RowBuffer{}
					out = buf
				}
				tr, err := newTableReader(&flowCtx, 0 /* processorID */, &ts, &c.post, out)
				if err != nil {
					t.Fatal(err)
				}

				var results execinfra.RowSource
				if rowSource {
					tr.Start(ctx)
					results = tr
				} else {
					tr.Run(ctx)
					if !buf.ProducerClosed() {
						t.Fatalf("output RowReceiver not closed")
					}
					buf.Start(ctx)
					results = buf
				}

				var res rowenc.EncDatumRows
				for {
					row, meta := results.Next()
					if meta != nil && meta.LeafTxnFinalState == nil && meta.Metrics == nil {
						t.Fatalf("unexpected metadata: %+v", meta)
					}
					if row == nil {
						break
					}
					res = append(res, row.Copy())
				}
				if result := res.String(tr.OutputTypes()); result != c.expected {
					t.Errorf("invalid results: %s, expected %s'", result, c.expected)
				}
			})
		})
	}
}

// Test that a TableReader outputs metadata about non-local ranges that it read.
func TestMisplannedRangesMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 3, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
			},
		})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	sqlutils.CreateTable(t, db, "t",
		"num INT PRIMARY KEY",
		3, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn))

	_, err := db.Exec(`
ALTER TABLE t SPLIT AT VALUES (1), (2), (3);
ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 1), (ARRAY[1], 2), (ARRAY[3], 3);
`)
	if err != nil {
		t.Fatal(err)
	}

	kvDB := tc.Server(0).DB()
	td := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	st := tc.Server(0).ClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	// Ensure the evalCtx is connected to the server's ID container, so they
	// are consistent with each other.
	evalCtx.NodeID = base.NewSQLIDContainerForNode(tc.Server(0).RPCContext().NodeID)

	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:   st,
			RangeCache: tc.Server(0).DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache(),
		},
		Txn:    kv.NewTxn(ctx, tc.Server(0).DB(), tc.Server(0).NodeID()),
		NodeID: evalCtx.NodeID,
	}
	post := execinfrapb.PostProcessSpec{}

	testutils.RunTrueAndFalse(t, "row-source", func(t *testing.T, rowSource bool) {
		spec := execinfrapb.TableReaderSpec{
			FetchSpec: makeFetchSpec(t, td, td.GetPrimaryIndex(), td.PublicColumns()[0].GetID()),
			Spans:     []roachpb.Span{td.PrimaryIndexSpan(keys.SystemSQLCodec)},
		}
		var out execinfra.RowReceiver
		var buf *distsqlutils.RowBuffer
		if !rowSource {
			buf = &distsqlutils.RowBuffer{}
			out = buf
		}
		tr, err := newTableReader(&flowCtx, 0 /* processorID */, &spec, &post, out)
		if err != nil {
			t.Fatal(err)
		}

		var results execinfra.RowSource
		if rowSource {
			tr.Start(ctx)
			results = tr
		} else {
			tr.Run(ctx)
			if !buf.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}
			buf.Start(ctx)
			results = buf
		}

		var res rowenc.EncDatumRows
		var metas []*execinfrapb.ProducerMetadata
		for {
			row, meta := results.Next()
			if meta != nil {
				metas = append(metas, meta)
				continue
			}
			if row == nil {
				break
			}
			res = append(res, row)
		}

		if len(res) != 3 {
			t.Fatalf("expected 3 rows, got: %d", len(res))
		}
		var misplannedRanges []roachpb.RangeInfo
		for _, m := range metas {
			if len(m.Ranges) > 0 {
				misplannedRanges = m.Ranges
			} else if m.LeafTxnFinalState == nil && m.Metrics == nil {
				t.Fatalf("expected only txn coord meta, metrics, or misplanned ranges, got: %+v", metas)
			}
		}
		if len(misplannedRanges) != 2 {
			t.Fatalf("expected 2 misplanned ranges, got: %+v", misplannedRanges)
		}
		// The metadata about misplanned ranges can come in any order (it depends on
		// the order in which parallel sub-batches complete after having been split by
		// DistSender).
		sort.Slice(misplannedRanges, func(i, j int) bool {
			return misplannedRanges[i].Lease.Replica.NodeID < misplannedRanges[j].Lease.Replica.NodeID
		})
		if misplannedRanges[0].Lease.Replica.NodeID != 2 ||
			misplannedRanges[1].Lease.Replica.NodeID != 3 {
			t.Fatalf("expected misplanned ranges from nodes 2 and 3, got: %+v", metas[0])
		}
	})
}

func TestTableReaderDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlutils.CreateTable(t, sqlDB, "t",
		"num INT PRIMARY KEY",
		3, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn))

	td := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	// Run the flow in a verbose trace so that we can test for tracing info.
	tracer := s.TracerI().(*tracing.Tracer)
	ctx, sp := tracer.StartSpanCtx(context.Background(), "test flow ctx", tracing.WithRecording(tracing.RecordingVerbose))
	defer sp.Finish()
	st := s.ClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	rootTxn := kv.NewTxn(ctx, s.DB(), s.NodeID())
	leafInputState := rootTxn.GetLeafTxnInputState(ctx)
	leafTxn := kv.NewLeafTxn(ctx, s.DB(), s.NodeID(), leafInputState)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		Txn:    leafTxn,
		Local:  true,
		NodeID: evalCtx.NodeID,
	}
	spec := execinfrapb.TableReaderSpec{
		Spans:     []roachpb.Span{td.PrimaryIndexSpan(keys.SystemSQLCodec)},
		FetchSpec: makeFetchSpec(t, td, td.GetPrimaryIndex(), td.PublicColumns()[0].GetID()),
	}
	post := execinfrapb.PostProcessSpec{}

	testReaderProcessorDrain(ctx, t, func(out execinfra.RowReceiver) (execinfra.Processor, error) {
		return newTableReader(&flowCtx, 0 /* processorID */, &spec, &post, out)
	})
}

// Test that a scan with a limit doesn't touch more ranges than necessary (i.e.
// we properly set the limit on the underlying Fetcher/KVFetcher).
func TestLimitScans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "test",
	})
	defer s.Stopper().Stop(ctx)

	sqlutils.CreateTable(t, sqlDB, "t",
		"num INT PRIMARY KEY",
		100, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn))

	if _, err := sqlDB.Exec("ALTER TABLE t SPLIT AT VALUES (5)"); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	st := s.ClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
			RangeCache: rangecache.NewRangeCache(s.ClusterSettings(), nil,
				func() int64 { return 2 << 10 }, s.Stopper(), s.TracerI().(*tracing.Tracer)),
		},
		Txn:    kv.NewTxn(ctx, kvDB, s.NodeID()),
		NodeID: evalCtx.NodeID,
	}
	spec := execinfrapb.TableReaderSpec{
		FetchSpec: makeFetchSpec(t, tableDesc, tableDesc.GetPrimaryIndex()),
		Spans:     []roachpb.Span{tableDesc.PrimaryIndexSpan(keys.SystemSQLCodec)},
	}
	// We're going to ask for 3 rows, all contained in the first range.
	const limit = 3
	post := execinfrapb.PostProcessSpec{Limit: limit}

	// Now we're going to run the tableReader and trace it.
	tracer := s.TracerI().(*tracing.Tracer)
	sp := tracer.StartSpan("root", tracing.WithRecording(tracing.RecordingVerbose))
	ctx = tracing.ContextWithSpan(ctx, sp)
	flowCtx.EvalCtx.Context = ctx
	flowCtx.CollectStats = true

	tr, err := newTableReader(&flowCtx, 0 /* processorID */, &spec, &post, nil /* output */)
	if err != nil {
		t.Fatal(err)
	}

	tr.Start(ctx)
	rows := 0
	for {
		row, meta := tr.Next()
		if row != nil {
			rows++
		}

		// Simulate what the DistSQLReceiver does and ingest the trace.
		if meta != nil && len(meta.TraceData) > 0 {
			sp.ImportRemoteSpans(meta.TraceData)
		}

		if row == nil && meta == nil {
			break
		}
	}
	if rows != limit {
		t.Fatalf("expected %d rows, got: %d", limit, rows)
	}

	skip.UnderMetamorphic(t, "the rest of this test isn't metamorphic: its output "+
		"depends on the batch size, which varies the number of spans searched.")

	// We're now going to count how many distinct scans we've done. This regex is
	// specific so that we don't count range resolving requests, and we dedupe
	// scans from the same key as the DistSender retries scans when it detects
	// splits.
	re := regexp.MustCompile(fmt.Sprintf(`querying next range at /Table/%d/1(\S.*)?`, tableDesc.GetID()))
	spans := sp.GetConfiguredRecording()
	ranges := make(map[string]struct{})
	for _, span := range spans {
		if span.Operation == tableReaderProcName {
			// Verify that stat collection lines up with results.
			stats := execinfrapb.ComponentStats{}
			span.Structured(func(item *types.Any, _ time.Time) {
				if !types.Is(item, &stats) {
					return
				}
				if err := types.UnmarshalAny(item, &stats); err != nil {
					t.Fatal(err)
				}
			})

			if stats.KV.TuplesRead.Value() != limit {
				t.Fatalf("read %d rows, but stats counted: %s", limit, stats.KV.TuplesRead)
			}
		}
		for _, l := range span.Logs {
			match := re.FindStringSubmatch(l.Msg().StripMarkers())
			if match == nil {
				continue
			}
			ranges[match[1]] = struct{}{}
		}
	}
	if len(ranges) != 1 {
		t.Fatalf("expected one ranges scanned, got: %d (%+v)", len(ranges), ranges)
	}
}

func BenchmarkTableReader(b *testing.B) {
	defer leaktest.AfterTest(b)()
	logScope := log.Scope(b)
	defer logScope.Close(b)
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	st := s.ClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	const numCols = 2
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		tableName := fmt.Sprintf("t%d", numRows)
		sqlutils.CreateTable(
			b, sqlDB, tableName,
			"k INT PRIMARY KEY, v INT",
			numRows,
			sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(42)),
		)
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", tableName)
		flowCtx := execinfra.FlowCtx{
			EvalCtx: &evalCtx,
			Cfg: &execinfra.ServerConfig{
				Settings: st,
				RangeCache: rangecache.NewRangeCache(s.ClusterSettings(), nil,
					func() int64 { return 2 << 10 }, s.Stopper(), s.TracerI().(*tracing.Tracer)),
			},
			Txn:    kv.NewTxn(ctx, s.DB(), s.NodeID()),
			NodeID: evalCtx.NodeID,
		}

		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			span := tableDesc.PrimaryIndexSpan(keys.SystemSQLCodec)
			spec := execinfrapb.TableReaderSpec{
				FetchSpec: makeFetchSpec(
					b, tableDesc, tableDesc.GetPrimaryIndex(),
					tableDesc.PublicColumns()[0].GetID(), tableDesc.PublicColumns()[1].GetID(),
				),
				// Spans will be set below.
			}
			post := execinfrapb.PostProcessSpec{}

			b.SetBytes(int64(numRows * numCols * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// We have to set the spans on each iteration since the
				// txnKVFetcher reuses the passed-in slice and destructively
				// modifies it.
				spec.Spans = []roachpb.Span{span}
				tr, err := newTableReader(&flowCtx, 0 /* processorID */, &spec, &post, nil /* output */)
				if err != nil {
					b.Fatal(err)
				}
				tr.Start(ctx)
				count := 0
				for {
					row, meta := tr.Next()
					if meta != nil && meta.LeafTxnFinalState == nil && meta.Metrics == nil {
						b.Fatalf("unexpected metadata: %+v", meta)
					}
					if row != nil {
						count++
					} else if meta == nil {
						break
					}
				}
				if count != numRows {
					b.Fatalf("found %d rows, expected %d", count, numRows)
				}
			}
		})
	}
}

func makeFetchSpec(
	t testing.TB, table catalog.TableDescriptor, index catalog.Index, colIDs ...descpb.ColumnID,
) descpb.IndexFetchSpec {
	var spec descpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(&spec, keys.SystemSQLCodec, table, index, colIDs); err != nil {
		t.Fatal(err)
	}
	return spec
}
