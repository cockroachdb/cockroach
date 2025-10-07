// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package resolvedspan_test

import (
	"context"
	"fmt"
	"iter"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/resolvedspan"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestAggregatorFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a fresh frontier with no progress.
	statementTime := makeTS(10)
	var initialHighwater hlc.Timestamp
	f, err := resolvedspan.NewAggregatorFrontier(
		statementTime,
		initialHighwater,
		mockCodec{},
		false, /* perTableTracking */
		makeSpan("a", "f"),
	)
	require.NoError(t, err)
	require.Equal(t, initialHighwater, f.Frontier())

	// Forward spans representing initial scan.
	testBackfillSpan(t, f, "a", "b", statementTime, initialHighwater)
	testBackfillSpan(t, f, "b", "f", statementTime, statementTime)

	// Forward spans signalling a backfill is required.
	backfillTS := makeTS(20)
	testBoundarySpan(t, f, "a", "b", backfillTS.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)
	testBoundarySpan(t, f, "b", "c", backfillTS.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)
	testBoundarySpan(t, f, "c", "d", backfillTS.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)
	testBoundarySpan(t, f, "d", "e", backfillTS.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)
	testBoundarySpan(t, f, "e", "f", backfillTS.Prev(), jobspb.ResolvedSpan_BACKFILL, backfillTS.Prev())

	// Verify that attempting to signal an earlier boundary causes an assertion error.
	illegalBoundaryTS := makeTS(15)
	testIllegalBoundarySpan(t, f, "a", "f", illegalBoundaryTS, jobspb.ResolvedSpan_BACKFILL)
	testIllegalBoundarySpan(t, f, "a", "f", illegalBoundaryTS, jobspb.ResolvedSpan_RESTART)
	testIllegalBoundarySpan(t, f, "a", "f", illegalBoundaryTS, jobspb.ResolvedSpan_EXIT)

	// Verify that attempting to signal a boundary at the latest boundary time with a different
	// boundary type causes an assertion error.
	testIllegalBoundarySpan(t, f, "a", "f", backfillTS.Prev(), jobspb.ResolvedSpan_RESTART)
	testIllegalBoundarySpan(t, f, "a", "f", backfillTS.Prev(), jobspb.ResolvedSpan_EXIT)

	// Forward spans representing actual backfill.
	testBackfillSpan(t, f, "d", "e", backfillTS, backfillTS.Prev())
	testBackfillSpan(t, f, "e", "f", backfillTS, backfillTS.Prev())
	testBackfillSpan(t, f, "a", "d", backfillTS, backfillTS)

	// Forward spans signalling a restart is required.
	restartTS := makeTS(30)
	testBoundarySpan(t, f, "a", "b", restartTS.Prev(), jobspb.ResolvedSpan_RESTART, backfillTS)
	testBoundarySpan(t, f, "b", "f", restartTS.Prev(), jobspb.ResolvedSpan_RESTART, restartTS.Prev())

	// Simulate restarting by creating a new frontier with the initial highwater
	// set to the previous frontier timestamp.
	initialHighwater = restartTS.Prev()
	f, err = resolvedspan.NewAggregatorFrontier(
		statementTime,
		initialHighwater,
		mockCodec{},
		false, /* perTableTracking */
		makeSpan("a", "f"),
	)
	require.NoError(t, err)

	// Forward spans representing post-restart backfill.
	testBackfillSpan(t, f, "a", "b", restartTS, initialHighwater)
	testBackfillSpan(t, f, "e", "f", restartTS, initialHighwater)
	testBackfillSpan(t, f, "b", "e", restartTS, restartTS)

	// Forward spans signalling an exit is required.
	exitTS := makeTS(40)
	testBoundarySpan(t, f, "a", "f", exitTS.Prev(), jobspb.ResolvedSpan_EXIT, exitTS.Prev())
}

func TestCoordinatorFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a fresh frontier with no progress.
	statementTime := makeTS(10)
	var initialHighwater hlc.Timestamp
	f, err := resolvedspan.NewCoordinatorFrontier(
		statementTime,
		initialHighwater,
		mockCodec{},
		false, /* perTableTracking */
		makeSpan("a", "f"),
	)
	require.NoError(t, err)
	require.Equal(t, initialHighwater, f.Frontier())

	// Forward spans representing initial scan.
	testBackfillSpan(t, f, "a", "b", statementTime, initialHighwater)
	testBackfillSpan(t, f, "b", "f", statementTime, statementTime)

	// Forward span signalling a backfill is required.
	backfillTS1 := makeTS(15)
	testBoundarySpan(t, f, "a", "b", backfillTS1.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)

	// Forward span signalling another backfill is required (simulates multiple
	// aggregators progressing at different speeds).
	backfillTS2 := makeTS(20)
	testBackfillSpan(t, f, "a", "b", backfillTS1, statementTime)
	testBoundarySpan(t, f, "a", "b", backfillTS2.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)

	// Verify that spans signalling backfills at earlier timestamp are allowed.
	testBoundarySpan(t, f, "b", "c", backfillTS1.Prev(), jobspb.ResolvedSpan_BACKFILL, statementTime)

	// Verify that no other boundary spans at earlier timestamp are allowed.
	testIllegalBoundarySpan(t, f, "a", "f", backfillTS1.Prev(), jobspb.ResolvedSpan_RESTART)
	testIllegalBoundarySpan(t, f, "a", "f", backfillTS1.Prev(), jobspb.ResolvedSpan_EXIT)

	// Verify that attempting to signal a boundary at the latest boundary time with a different
	// boundary type causes an assertion error.
	testIllegalBoundarySpan(t, f, "a", "f", backfillTS2.Prev(), jobspb.ResolvedSpan_RESTART)
	testIllegalBoundarySpan(t, f, "a", "f", backfillTS2.Prev(), jobspb.ResolvedSpan_EXIT)

	// Forward spans completing first backfill and signalling and completing second backfill.
	testBackfillSpan(t, f, "b", "f", backfillTS1, backfillTS1)
	testBoundarySpan(t, f, "b", "f", backfillTS2.Prev(), jobspb.ResolvedSpan_BACKFILL, backfillTS2.Prev())
	testBackfillSpan(t, f, "a", "f", backfillTS2, backfillTS2)

	// Forward spans signalling a restart is required.
	restartTS := makeTS(30)
	testBoundarySpan(t, f, "a", "b", restartTS.Prev(), jobspb.ResolvedSpan_RESTART, backfillTS2)
	testBoundarySpan(t, f, "b", "f", restartTS.Prev(), jobspb.ResolvedSpan_RESTART, restartTS.Prev())

	// Simulate restarting by creating a new frontier with the initial highwater
	// set to the previous frontier timestamp.
	initialHighwater = restartTS.Prev()
	f, err = resolvedspan.NewCoordinatorFrontier(
		statementTime,
		initialHighwater,
		mockCodec{},
		false, /* perTableTracking */
		makeSpan("a", "f"),
	)
	require.NoError(t, err)

	// Forward spans representing post-restart backfill.
	testBackfillSpan(t, f, "a", "b", restartTS, initialHighwater)
	testBackfillSpan(t, f, "e", "f", restartTS, initialHighwater)
	testBackfillSpan(t, f, "b", "e", restartTS, restartTS)

	// Forward spans signalling an exit is required.
	exitTS := makeTS(40)
	testBoundarySpan(t, f, "a", "f", exitTS.Prev(), jobspb.ResolvedSpan_EXIT, exitTS.Prev())
}

type frontier interface {
	AddSpansAt(startAt hlc.Timestamp, spans ...roachpb.Span) error
	Frontier() hlc.Timestamp
	ForwardResolvedSpan(jobspb.ResolvedSpan) (bool, error)
	InBackfill(jobspb.ResolvedSpan) bool
	AtBoundary() (bool, jobspb.ResolvedSpan_BoundaryType, hlc.Timestamp)
	All() iter.Seq[jobspb.ResolvedSpan]
	Frontiers() iter.Seq2[descpb.ID, span.ReadOnlyFrontier]
}

func testBackfillSpan(
	t *testing.T, f frontier, start, end string, ts hlc.Timestamp, frontierAfterSpan hlc.Timestamp,
) {
	backfillSpan := makeResolvedSpan(start, end, ts, jobspb.ResolvedSpan_NONE)
	require.True(t, f.InBackfill(backfillSpan))
	_, err := f.ForwardResolvedSpan(backfillSpan)
	require.NoError(t, err)
	require.Equal(t, frontierAfterSpan, f.Frontier())
}

func testBoundarySpan(
	t *testing.T,
	f frontier,
	start, end string,
	boundaryTS hlc.Timestamp,
	boundaryType jobspb.ResolvedSpan_BoundaryType,
	frontierAfterSpan hlc.Timestamp,
) {
	boundarySpan := makeResolvedSpan(start, end, boundaryTS, boundaryType)
	_, err := f.ForwardResolvedSpan(boundarySpan)
	require.NoError(t, err)

	if finalBoundarySpan := frontierAfterSpan.Equal(boundaryTS); finalBoundarySpan {
		atBoundary, bType, bTS := f.AtBoundary()
		require.True(t, atBoundary)
		require.Equal(t, boundaryType, bType)
		require.Equal(t, boundaryTS, bTS)
		for resolvedSpan := range f.All() {
			require.Equal(t, boundaryTS, resolvedSpan.Timestamp)
			require.Equal(t, boundaryType, resolvedSpan.BoundaryType)
		}
	} else {
		atBoundary, _, _ := f.AtBoundary()
		require.False(t, atBoundary)
		for resolvedSpan := range f.All() {
			if resolvedSpan.Span.Contains(makeSpan(start, end)) {
				require.Equal(t, boundaryTS, resolvedSpan.Timestamp)
				require.Equal(t, boundaryType, resolvedSpan.BoundaryType)
				break
			}
		}
	}
}

func testIllegalBoundarySpan(
	t *testing.T,
	f frontier,
	start, end string,
	boundaryTS hlc.Timestamp,
	boundaryType jobspb.ResolvedSpan_BoundaryType,
) {
	boundarySpan := makeResolvedSpan(start, end, boundaryTS, boundaryType)
	_, err := f.ForwardResolvedSpan(boundarySpan)
	require.True(t, errors.HasAssertionFailure(err))
}

func makeTS(wt int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: wt}
}

func makeResolvedSpan(
	start, end string, ts hlc.Timestamp, boundaryType jobspb.ResolvedSpan_BoundaryType,
) jobspb.ResolvedSpan {
	return jobspb.ResolvedSpan{
		Span:         makeSpan(start, end),
		Timestamp:    ts,
		BoundaryType: boundaryType,
	}
}

func makeSpan(start, end string) roachpb.Span {
	return roachpb.Span{
		Key:    roachpb.Key(start),
		EndKey: roachpb.Key(end),
	}
}

func TestAggregatorFrontier_ForwardResolvedSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a fresh frontier with no progress.
	f, err := resolvedspan.NewAggregatorFrontier(
		hlc.Timestamp{},
		hlc.Timestamp{},
		mockCodec{},
		false, /* perTableTracking */
		makeSpan("a", "f"),
	)
	require.NoError(t, err)
	require.Zero(t, f.Frontier())

	t.Run("advance frontier with no boundary", func(t *testing.T) {
		// Forwarding part of the span space to 10 should not advance the frontier.
		forwarded, err := f.ForwardResolvedSpan(
			makeResolvedSpan("a", "b", makeTS(10), jobspb.ResolvedSpan_NONE))
		require.NoError(t, err)
		require.False(t, forwarded)
		require.Zero(t, f.Frontier())

		// Forwarding the rest of the span space to 10 should advance the frontier.
		forwarded, err = f.ForwardResolvedSpan(
			makeResolvedSpan("b", "f", makeTS(10), jobspb.ResolvedSpan_NONE))
		require.NoError(t, err)
		require.True(t, forwarded)
		require.Equal(t, makeTS(10), f.Frontier())
	})

	t.Run("advance frontier with same timestamp and new boundary", func(t *testing.T) {
		// Forwarding part of the span space to 10 again with a non-NONE boundary
		// should be considered forwarding the frontier because we're learning
		// about a new boundary.
		forwarded, err := f.ForwardResolvedSpan(
			makeResolvedSpan("c", "f", makeTS(10), jobspb.ResolvedSpan_RESTART))
		require.NoError(t, err)
		require.True(t, forwarded)
		require.Equal(t, makeTS(10), f.Frontier())

		// Forwarding the rest of the span space to 10 again with a non-NONE boundary
		// should not be considered forwarding the frontier because we already
		// know about the new boundary.
		forwarded, err = f.ForwardResolvedSpan(
			makeResolvedSpan("a", "c", makeTS(10), jobspb.ResolvedSpan_RESTART))
		require.NoError(t, err)
		require.False(t, forwarded)
		require.Equal(t, makeTS(10), f.Frontier())
	})
}

// mockCodec is a simple TableCodec for testing
// that treats all keys as table ID 1.
type mockCodec struct{}

var _ resolvedspan.TableCodec = mockCodec{}

// DecodeTablePrefix implements TableCodec.
func (mockCodec) DecodeTablePrefix(key roachpb.Key) ([]byte, uint32, error) {
	return key, 1, nil
}

// TableSpan implements TableCodec.
func (mockCodec) TableSpan(tableID uint32) roachpb.Span {
	if tableID == 1 {
		// Since the mock codec treats all keys as belonging to table ID 1,
		// we return the everything span so that all keys will be considered
		// a part of the table.
		return keys.EverythingSpan
	}
	panic("mock codec only handles table ID 1")
}

func TestFrontierPerTableResolvedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, frontierType := range []string{"aggregator", "coordinator"} {
		t.Run(frontierType, func(t *testing.T) {
			rnd, _ := randutil.NewPseudoRand()

			// Randomly use either the system codec or a tenant codec.
			codec := func() keys.SQLCodec {
				if rnd.Float64() < 0.5 {
					tenantID := roachpb.MustMakeTenantID(uint64(1 + rnd.Intn(10)))
					return keys.MakeSQLCodec(tenantID)
				}
				return keys.SystemSQLCodec
			}()

			// Helper to create spans for tables.
			tableSpan := func(tableID uint32) roachpb.Span {
				// Randomly choose either the full table span or an index span.
				if rnd.Float64() < 0.5 {
					return codec.TableSpan(tableID)
				}
				prefix := codec.IndexPrefix(tableID, 1 /* indexID */)
				return roachpb.Span{
					Key:    prefix,
					EndKey: prefix.PrefixEnd(),
				}
			}

			// Create spans for multiple tables (table IDs 10, 20, 30).
			table10Span := tableSpan(10)
			table20Span := tableSpan(20)
			table30Span := tableSpan(30)
			tableSpans := []roachpb.Span{table10Span, table20Span, table30Span}

			statementTime := makeTS(5)
			initialHighWater := hlc.Timestamp{}

			// Create frontier with multiple table spans.
			f, err := func() (frontier, error) {
				switch frontierType {
				case "aggregator":
					return resolvedspan.NewAggregatorFrontier(
						statementTime,
						initialHighWater,
						codec,
						true, /* perTableTracking */
						tableSpans...,
					)
				case "coordinator":
					return resolvedspan.NewCoordinatorFrontier(
						statementTime,
						initialHighWater,
						codec,
						true, /* perTableTracking */
						tableSpans...,
					)
				default:
					t.Fatalf("unknown frontier type: %s", frontierType)
				}
				panic("unreachable")
			}()
			require.NoError(t, err)
			require.Equal(t, initialHighWater, f.Frontier())

			// Forward table 10 to timestamp 10.
			_, err = f.ForwardResolvedSpan(jobspb.ResolvedSpan{
				Span:      table10Span,
				Timestamp: makeTS(10),
			})
			require.NoError(t, err)

			// Forward table 20 to timestamp 15.
			_, err = f.ForwardResolvedSpan(jobspb.ResolvedSpan{
				Span:      table20Span,
				Timestamp: makeTS(15),
			})
			require.NoError(t, err)

			// Forward table 30 to timestamp 8.
			_, err = f.ForwardResolvedSpan(jobspb.ResolvedSpan{
				Span:      table30Span,
				Timestamp: makeTS(8),
			})
			require.NoError(t, err)

			// Overall frontier should be the minimum (table 30 at timestamp 8).
			require.Equal(t, makeTS(8), f.Frontier())

			// Verify per-table resolved timestamps.
			perTableResolved := make(map[uint32]hlc.Timestamp)
			for tableID, frontier := range f.Frontiers() {
				perTableResolved[uint32(tableID)] = frontier.Frontier()
			}
			require.Equal(t, makeTS(10), perTableResolved[10])
			require.Equal(t, makeTS(15), perTableResolved[20])
			require.Equal(t, makeTS(8), perTableResolved[30])

			// Forward table 30 to catch up.
			_, err = f.ForwardResolvedSpan(jobspb.ResolvedSpan{
				Span:      table30Span,
				Timestamp: makeTS(12),
			})
			require.NoError(t, err)

			// Overall frontier should now advance to table 10's timestamp (10).
			require.Equal(t, makeTS(10), f.Frontier())

			// Verify per-table resolved timestamps again.
			perTableResolved = make(map[uint32]hlc.Timestamp)
			for tableID, frontier := range f.Frontiers() {
				perTableResolved[uint32(tableID)] = frontier.Frontier()
			}
			require.Equal(t, makeTS(10), perTableResolved[10])
			require.Equal(t, makeTS(15), perTableResolved[20])
			require.Equal(t, makeTS(12), perTableResolved[30])
		})
	}
}

func TestFrontierForwardFullTableSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "frontier type", []string{"aggregator", "coordinator"},
		func(t *testing.T, frontierType string) {
			rnd, _ := randutil.NewPseudoRand()

			// Randomly use either the system codec or a tenant codec.
			codec := func() keys.SQLCodec {
				if rnd.Float64() < 0.5 {
					tenantID := roachpb.MustMakeTenantID(uint64(1 + rnd.Intn(10)))
					return keys.MakeSQLCodec(tenantID)
				}
				return keys.SystemSQLCodec
			}()

			key := func(base roachpb.Key, suffix string) roachpb.Key {
				result := make([]byte, len(base)+len(suffix))
				copy(result, base)
				copy(result[len(base):], suffix)
				return result
			}

			// Create spans for tables 109 and 110.
			// These table IDs were specifically chosen because 109->110
			// is the boundary for when table IDs go from single-byte to
			// multi-byte encodings.
			table109Span := codec.TableSpan(109)
			table110Span := codec.TableSpan(110)
			require.True(t, len(table110Span.Key) == len(table109Span.Key)+1)

			// Create several subspans within each table.
			tableSpans := []roachpb.Span{
				{Key: key(table109Span.Key, "a"), EndKey: key(table109Span.Key, "c")},
				{Key: key(table109Span.Key, "e"), EndKey: key(table109Span.Key, "g")},
				{Key: key(table110Span.Key, "b"), EndKey: key(table110Span.Key, "d")},
				{Key: key(table110Span.Key, "f"), EndKey: key(table110Span.Key, "k")},
			}

			statementTime := makeTS(5)
			var initialHighWater hlc.Timestamp

			f, err := func() (span.Frontier, error) {
				switch frontierType {
				case "aggregator":
					return resolvedspan.NewAggregatorFrontier(
						statementTime,
						initialHighWater,
						codec,
						true, /* perTableTracking */
						tableSpans...,
					)
				case "coordinator":
					return resolvedspan.NewCoordinatorFrontier(
						statementTime,
						initialHighWater,
						codec,
						true, /* perTableTracking */
						tableSpans...,
					)
				default:
					t.Fatalf("unknown frontier type: %s", frontierType)
				}
				panic("unreachable")
			}()
			require.NoError(t, err)
			require.Equal(t, initialHighWater, f.Frontier())

			// Forward both tables to timestamp 20.
			targetTimestamp := makeTS(20)
			for _, tableSpan := range []roachpb.Span{table109Span, table110Span} {
				_, err := f.Forward(tableSpan, targetTimestamp)
				require.NoError(t, err)
			}
			require.Equal(t, targetTimestamp, f.Frontier())
		})
}

func BenchmarkFrontierPerTableTracking(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()

	// Generate a random span iteration order that's the same for all
	// sub-benchmarks.
	const numSpans = 100
	order := make([]int, numSpans)
	for i := range numSpans {
		order[i] = i
	}
	rng.Shuffle(numSpans, func(i, j int) {
		order[i], order[j] = order[j], order[i]
	})

	for _, tenantType := range []string{"system", "tenant"} {
		for _, frontierType := range []string{"aggregator", "coordinator"} {
			for _, perTableTracking := range []bool{false, true} {
				b.Run(
					fmt.Sprintf("%s/%s/per-table-tracking=%t", tenantType, frontierType, perTableTracking),
					func(b *testing.B) {
						// Start the server.
						srv, db, _ := serverutils.StartServer(b, base.TestServerArgs{
							DefaultTestTenant: base.TestControlsTenantsExplicitly,
						})
						defer srv.Stopper().Stop(ctx)

						// Get a SQL connection/codec for the tenant type.
						sqlDB, codec := func() (*sqlutils.SQLRunner, keys.SQLCodec) {
							switch tenantType {
							case "system":
								return sqlutils.MakeSQLRunner(db), keys.SystemSQLCodec
							case "tenant":
								tenantID := roachpb.MinTenantID
								_, tenantDB := serverutils.StartTenant(b, srv, base.TestTenantArgs{
									TenantID: tenantID,
								})
								return sqlutils.MakeSQLRunner(tenantDB), keys.MakeSQLCodec(tenantID)
							default:
								panic("unreachable")
							}
						}()

						// Create a table and split it into multiple spans.
						sqlDB.Exec(b, `CREATE TABLE foo (id INT PRIMARY KEY)`)
						sqlDB.Exec(b, fmt.Sprintf(
							`ALTER TABLE foo SPLIT AT SELECT generate_series(10, %d, 10)`, (numSpans-1)*10))

						var fooTableID uint32
						sqlDB.QueryRow(b, `SELECT 'foo'::regclass::oid::int`).Scan(&fooTableID)
						fooSpan := codec.TableSpan(fooTableID)

						// Collect all the spans.
						var spans roachpb.Spans
						rows := sqlDB.Query(b, `SELECT raw_start_key, raw_end_key
FROM [SHOW RANGES FROM TABLE foo WITH KEYS]`)
						for rows.Next() {
							var startKey, endKey roachpb.Key
							err := rows.Scan(&startKey, &endKey)
							require.NoError(b, err)
							sp := roachpb.Span{Key: startKey, EndKey: endKey}
							spans = append(spans, fooSpan.Intersect(sp))
						}
						require.Len(b, spans, numSpans)

						now := makeTS(timeutil.Now().Unix())

						// Create the frontier and add all the spans.
						f, err := func() (frontier, error) {
							switch frontierType {
							case "aggregator":
								return resolvedspan.NewAggregatorFrontier(
									now,
									now,
									codec,
									perTableTracking,
								)
							case "coordinator":
								return resolvedspan.NewCoordinatorFrontier(
									now,
									now,
									codec,
									perTableTracking,
								)
							default:
								panic("unreachable")
							}
						}()
						require.NoError(b, err)
						require.NoError(b, f.AddSpansAt(now, spans...))

						// Main benchmark loop: forward (shuffled) spans in a loop.
						b.ResetTimer()
						for n := range b.N {
							ts := now.AddDuration(time.Second)
							i := n % len(spans)
							_, err := f.ForwardResolvedSpan(jobspb.ResolvedSpan{
								Span:      spans[order[i]],
								Timestamp: ts,
							})
							if err != nil {
								b.Fatalf("error forwarding: %v", err)
							}
						}
						b.StopTimer()

						// Make sure the compiler doesn't optimize away the forwards.
						require.True(b, now.LessEq(f.Frontier()))
					})
			}
		}
	}
}
