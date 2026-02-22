// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package exec

import (
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

func TestMetricsRecordJoinType(t *testing.T) {
	var m QueryMetrics

	m.RecordJoinType(descpb.InnerJoin)
	if c := m.JoinTypeCounts[descpb.InnerJoin]; c != 1 {
		t.Errorf("expected 1 inner join, got %d", c)
	}

	// Left and right outer joins are recorded as left outer joins.
	m.RecordJoinType(descpb.LeftOuterJoin)
	m.RecordJoinType(descpb.RightOuterJoin)
	if c := m.JoinTypeCounts[descpb.LeftOuterJoin]; c != 2 {
		t.Errorf("expected 2 left outer joins, got %d", c)
	}

	// Left and right semi joins are recorded as left anti joins.
	m.RecordJoinType(descpb.LeftSemiJoin)
	m.RecordJoinType(descpb.RightSemiJoin)
	if c := m.JoinTypeCounts[descpb.LeftSemiJoin]; c != 2 {
		t.Errorf("expected 2 left semi joins, got %d", c)
	}

	// Left and right anti joins are recorded as left anti joins.
	m.RecordJoinType(descpb.LeftAntiJoin)
	m.RecordJoinType(descpb.RightAntiJoin)
	if c := m.JoinTypeCounts[descpb.LeftAntiJoin]; c != 2 {
		t.Errorf("expected 2 left anti joins, got %d", c)
	}

	// Up to 255 joins of a given type are recorded and should not overflow.
	for i := 0; i < math.MaxUint8+10; i++ {
		m.RecordJoinType(descpb.InnerJoin)
	}
	if c := m.JoinTypeCounts[descpb.InnerJoin]; c != math.MaxUint8 {
		t.Errorf("expected %d inner joins, got %d", math.MaxUint8, c)
	}
}

func TestMetricsRecordJoinAlgorithm(t *testing.T) {
	var m QueryMetrics

	// Up to 255 joins of a given algorithm are recorded and should not
	// overflow.
	for i := 0; i < math.MaxUint8+10; i++ {
		m.RecordJoinAlgorithm(HashJoin)
	}
	if c := m.JoinAlgorithmCounts[HashJoin]; c != math.MaxUint8 {
		t.Errorf("expected %d hash joins, got %d", math.MaxUint8, c)
	}

}

func TestIndexesUsed(t *testing.T) {
	var iu IndexesUsed
	iu.Add(1, 1)
	iu.Add(1, 2)
	iu.Add(2, 1)
	iu.Add(1, 1) // duplicate

	s := iu.Strings()
	if !reflect.DeepEqual(s, []string{"1@1", "1@2", "2@1"}) {
		t.Errorf("unexpected indexes: %v", s)
	}
}
