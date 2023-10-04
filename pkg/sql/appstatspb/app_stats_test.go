// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package appstatspb

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestAddNumericStats(t *testing.T) {
	var a, b, ab NumericStat
	var countA, countB, countAB int64
	var sumA, sumB, sumAB float64

	aData := []float64{1.1, 3.3, 2.2}
	bData := []float64{2.0, 3.0, 5.5, 1.2}

	// Feed some data to A.
	for _, v := range aData {
		countA++
		sumA += v
		a.Record(countA, v)
	}

	// Feed some data to B.
	for _, v := range bData {
		countB++
		sumB += v
		b.Record(countB, v)
	}

	// Feed the A and B data to AB.
	for _, v := range append(bData, aData...) {
		countAB++
		sumAB += v
		ab.Record(countAB, v)
	}

	const epsilon = 0.0000001

	// Sanity check that we have non-trivial stats to combine.
	if mean := 2.2; math.Abs(mean-a.Mean) > epsilon {
		t.Fatalf("Expected Mean %f got %f", mean, a.Mean)
	}
	if mean := sumA / float64(countA); math.Abs(mean-a.Mean) > epsilon {
		t.Fatalf("Expected Mean %f got %f", mean, a.Mean)
	}
	if mean := sumB / float64(countB); math.Abs(mean-b.Mean) > epsilon {
		t.Fatalf("Expected Mean %f got %f", mean, b.Mean)
	}
	if mean := sumAB / float64(countAB); math.Abs(mean-ab.Mean) > epsilon {
		t.Fatalf("Expected Mean %f got %f", mean, ab.Mean)
	}

	// Verify that A+B = AB -- that the stat we get from combining the two is the
	// same as the one that saw the union of values the two saw.
	combined := AddNumericStats(a, b, countA, countB)
	if e := math.Abs(combined.Mean - ab.Mean); e > epsilon {
		t.Fatalf("Mean of combined %f does not match ab %f (%f)", combined.Mean, ab.Mean, e)
	}
	if e := combined.SquaredDiffs - ab.SquaredDiffs; e > epsilon {
		t.Fatalf("SquaredDiffs of combined %f does not match ab %f (%f)", combined.SquaredDiffs, ab.SquaredDiffs, e)
	}

	reversed := AddNumericStats(b, a, countB, countA)
	if combined != reversed {
		t.Fatalf("a+b != b+a: %v vs %v", combined, reversed)
	}

	// Check the in-place side-effect version matches the standalone helper.
	a.Add(b, countA, countB)
	if a != combined {
		t.Fatalf("a.Add(b) should match add(a, b): %+v vs %+v", a, combined)
	}
}

func TestAddExecStats(t *testing.T) {
	numericStatA := NumericStat{Mean: 354.123, SquaredDiffs: 34.34123}
	numericStatB := NumericStat{Mean: 9.34354, SquaredDiffs: 75.321}
	a := ExecStats{Count: 3, NetworkBytes: numericStatA}
	b := ExecStats{Count: 1, NetworkBytes: numericStatB}
	expectedNumericStat := AddNumericStats(a.NetworkBytes, b.NetworkBytes, a.Count, b.Count)
	a.Add(b)
	require.Equal(t, int64(4), a.Count)
	epsilon := 0.00000001
	require.True(t, expectedNumericStat.AlmostEqual(a.NetworkBytes, epsilon), "expected %+v, but found %+v", expectedNumericStat, a.NetworkMessages)
}

func TestToAggregatedStatementStatistics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Construct by hand a CollectedStatementStatistics struct. The values don't matter, but the same values
	// being included in the transformation result do. This will fail whenever someone makes a change to
	// obspb.AggregatedStatementStatistics, forcing folks to update the transformation logic accordingly.
	s := CollectedStatementStatistics{
		ID: StmtFingerprintID(12345),
		Key: StatementStatisticsKey{
			Query:                    "SELECT 1",
			App:                      "myApp",
			DistSQL:                  true,
			Failed:                   true,
			ImplicitTxn:              true,
			Vec:                      true,
			FullScan:                 true,
			Database:                 "my_db",
			PlanHash:                 uint64(81974),
			QuerySummary:             "summary",
			TransactionFingerprintID: TransactionFingerprintID(89743),
		},
		Stats: StatementStatistics{
			Count:             472,
			FirstAttemptCount: 13,
			MaxRetries:        45,
			NumRows: NumericStat{
				Mean:         142987.3,
				SquaredDiffs: 74126.4,
			},
			IdleLat: NumericStat{
				Mean:         129478.4,
				SquaredDiffs: 2986745.5,
			},
			ParseLat: NumericStat{
				Mean:         128974.4,
				SquaredDiffs: 8943.4,
			},
			PlanLat: NumericStat{
				Mean:         849712.3,
				SquaredDiffs: 1874.4,
			},
			RunLat: NumericStat{
				Mean:         891267.4,
				SquaredDiffs: 4189423.3,
			},
			ServiceLat: NumericStat{
				Mean:         18674213.4,
				SquaredDiffs: 128946712.4,
			},
			OverheadLat: NumericStat{
				Mean:         189274124.4,
				SquaredDiffs: 42176843.3,
			},
			SensitiveInfo: SensitiveInfo{
				LastErr:                 "last error",
				MostRecentPlanTimestamp: time.Date(2023, time.October, 5, 16, 55, 39, 0, time.UTC),
			},
			BytesRead: NumericStat{
				Mean:         12471.5,
				SquaredDiffs: 189675.34,
			},
			RowsRead: NumericStat{
				Mean:         128957643.4,
				SquaredDiffs: 15861535.4,
			},
			RowsWritten: NumericStat{
				Mean:         18567151.4,
				SquaredDiffs: 21.5,
			},
			ExecStats: ExecStats{
				Count: 19487,
				NetworkBytes: NumericStat{
					Mean:         4187.4,
					SquaredDiffs: 24.541,
				},
				MaxMemUsage: NumericStat{
					Mean:         4.5,
					SquaredDiffs: 155.4,
				},
				ContentionTime: NumericStat{
					Mean:         12948.4,
					SquaredDiffs: 12415.4,
				},
				NetworkMessages: NumericStat{
					Mean:         9814.4,
					SquaredDiffs: 1857.4,
				},
				MaxDiskUsage: NumericStat{
					Mean:         17944.5,
					SquaredDiffs: 108214.4,
				},
				CPUSQLNanos: NumericStat{
					Mean:         1924.3,
					SquaredDiffs: 120984.44,
				},
				MVCCIteratorStats: MVCCIteratorStats{
					StepCount: NumericStat{
						Mean:         12515.3,
						SquaredDiffs: 109871.5,
					},
					StepCountInternal: NumericStat{
						Mean:         1289471.4,
						SquaredDiffs: 67353246.5,
					},
					SeekCount: NumericStat{
						Mean:         74574.5,
						SquaredDiffs: 129875.4,
					},
					SeekCountInternal: NumericStat{
						Mean:         84754.5,
						SquaredDiffs: 571675.4,
					},
					BlockBytes: NumericStat{
						Mean:         4745634.5,
						SquaredDiffs: 8573.5,
					},
					BlockBytesInCache: NumericStat{
						Mean:         4871.5,
						SquaredDiffs: 757.4,
					},
					KeyBytes: NumericStat{
						Mean:         483754.5,
						SquaredDiffs: 85733.4,
					},
					ValueBytes: NumericStat{
						Mean:         41745.4,
						SquaredDiffs: 478374.5,
					},
					PointCount: NumericStat{
						Mean:         375465.54,
						SquaredDiffs: 41674.5,
					},
					PointsCoveredByRangeTombstones: NumericStat{
						Mean:         32745.5,
						SquaredDiffs: 24655.4,
					},
					RangeKeyCount: NumericStat{
						Mean:         214.4,
						SquaredDiffs: 423.34,
					},
					RangeKeyContainedPoints: NumericStat{
						Mean:         564.4,
						SquaredDiffs: 785.4,
					},
					RangeKeySkippedPoints: NumericStat{
						Mean:         21.4,
						SquaredDiffs: 4.5,
					},
				},
			},
			SQLType:              "sql type",
			LastExecTimestamp:    time.Date(2023, time.October, 5, 18, 33, 39, 0, time.UTC),
			Nodes:                []int64{1, 3, 4},
			Regions:              []string{"us-east-1", "us-west-2"},
			PlanGists:            []string{"plan 1 gist", "plan 2 gist"},
			IndexRecommendations: []string{"rec 1", "rec 2"},
			Indexes:              []string{"index 1", "index 2"},
			LatencyInfo: LatencyInfo{
				Min: 12.3,
				Max: 485.4,
				P50: 123.4,
				P90: 245.3,
				P99: 380.3,
			},
			LastErrorCode: "last err code",
		},
		AggregatedTs:        time.Date(2023, time.October, 5, 17, 0, 0, 0, time.UTC),
		AggregationInterval: 10 * time.Minute,
	}

	// NB: Make sure the nesting here is deep enough to test the recursive bits of
	// the transformation logic.
	s.Stats.SensitiveInfo.MostRecentPlanDescription = ExplainTreePlanNode{
		Name: "plan 1",
		Attrs: []*ExplainTreePlanNode_Attr{
			{
				Key:   "p1k1",
				Value: "p1v1",
			},
			{
				Key:   "p1k2",
				Value: "p1v2",
			},
		},
		Children: []*ExplainTreePlanNode{
			{
				Name: "plan 2",
				Attrs: []*ExplainTreePlanNode_Attr{
					{
						Key:   "p2k1",
						Value: "p2v1",
					},
					{
						Key:   "p2k2",
						Value: "p2v2",
					},
				},
				Children: []*ExplainTreePlanNode{
					{
						Name: "plan 3",
						Attrs: []*ExplainTreePlanNode_Attr{
							{
								Key:   "p3k1",
								Value: "p3v1",
							},
							{
								Key:   "p3k2",
								Value: "p3v2",
							},
						},
						Children: []*ExplainTreePlanNode{},
					},
				},
			},
			{
				Name: "plan 4",
				Attrs: []*ExplainTreePlanNode_Attr{
					{
						Key:   "p4k1",
						Value: "p4v1",
					},
					{
						Key:   "p4k2",
						Value: "p4v2",
					},
				},
				Children: []*ExplainTreePlanNode{
					{
						Name: "plan 5",
						Attrs: []*ExplainTreePlanNode_Attr{
							{
								Key:   "p5k1",
								Value: "p5v1",
							},
							{
								Key:   "p5k2",
								Value: "p5v2",
							},
						},
						Children: []*ExplainTreePlanNode{},
					},
				},
			},
		},
	}

	datadriven.RunTest(t, "testdata/collectedstmtstats_transform", func(t *testing.T, d *datadriven.TestData) string {
		transformed := s.ToAggregatedStatementStatistics()
		var buf bytes.Buffer
		_, err := fmt.Fprintf(&buf, "%# v\n", pretty.Formatter(transformed))
		require.NoError(t, err)
		return buf.String()
	})
}
