// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package props

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestHistogram(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	histData := []cat.HistogramBucket{
		{NumRange: 0, NumEq: 1, UpperBound: tree.NewDInt(1)},
		{NumRange: 3, NumEq: 3, UpperBound: tree.NewDInt(10)},
		{NumRange: 4, NumEq: 5, UpperBound: tree.NewDInt(25)},
		{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(30)},
		{NumRange: 40, NumEq: 35, UpperBound: tree.NewDInt(42)},
	}
	h := &Histogram{}
	h.Init(&evalCtx, opt.ColumnID(1), histData)

	testData := []struct {
		constraint string
		buckets    []HistogramBucket
		count      float64
	}{
		{
			constraint: "/1: [/0 - /0]",
			buckets: []HistogramBucket{
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(0)},
			},
			count: 0,
		},
		{
			constraint: "/1: [/50 - /100]",
			buckets: []HistogramBucket{
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(42)},
			},
			count: 0,
		},
		{
			constraint: "/1: [ - /1] [/11 - /24] [/30 - /45]",
			buckets: []HistogramBucket{
				{NumRange: 0, NumEq: 1, UpperBound: tree.NewDInt(1)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(10)},
				{NumRange: 4 * 13.0 / 14.0, NumEq: 4 * 1.0 / 14.0, UpperBound: tree.NewDInt(24)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 40, NumEq: 35, UpperBound: tree.NewDInt(42)},
			},
			count: 80,
		},
		{
			constraint: "/1: [/5 - /10] [/15 - /32] [/34 - /36] [/38 - ]",
			buckets: []HistogramBucket{
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(4)},
				{NumRange: 3 * 5.0 / 8.0, NumEq: 3, UpperBound: tree.NewDInt(10)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(14)},
				{NumRange: 4 * 10.0 / 14.0, NumEq: 5, UpperBound: tree.NewDInt(25)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 40.0 / 11.0, NumEq: 40.0 / 11.0, UpperBound: tree.NewDInt(32)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(33)},
				{NumRange: 2 * 40.0 / 11.0, NumEq: 40.0 / 11.0, UpperBound: tree.NewDInt(36)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(37)},
				{NumRange: 4 * 40.0 / 11.0, NumEq: 35, UpperBound: tree.NewDInt(42)},
			},
			count: 3*5.0/8.0 + 4*10.0/14.0 + 9*40.0/11 + 43,
		},
		{
			constraint: "/1: [ - /41]",
			buckets: []HistogramBucket{
				{NumRange: 0, NumEq: 1, UpperBound: tree.NewDInt(1)},
				{NumRange: 3, NumEq: 3, UpperBound: tree.NewDInt(10)},
				{NumRange: 4, NumEq: 5, UpperBound: tree.NewDInt(25)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 10 * 40.0 / 11.0, NumEq: 40.0 / 11.0, UpperBound: tree.NewDInt(41)},
			},
			count: 56,
		},
		{
			constraint: "/1: [/1 - ]",
			buckets: []HistogramBucket{
				{NumRange: 0, NumEq: 1, UpperBound: tree.NewDInt(1)},
				{NumRange: 3, NumEq: 3, UpperBound: tree.NewDInt(10)},
				{NumRange: 4, NumEq: 5, UpperBound: tree.NewDInt(25)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 40, NumEq: 35, UpperBound: tree.NewDInt(42)},
			},
			count: 91,
		},
	}

	for i := range testData {
		c := constraint.ParseConstraint(&evalCtx, testData[i].constraint)
		if !h.CanFilter(&c) {
			t.Fatalf("constraint %s cannot filter histogram %v", c.String(), *h)
		}
		filtered := h.Filter(&c)
		if !reflect.DeepEqual(testData[i].buckets, filtered.buckets) {
			t.Fatalf("expected %v but found %v", testData[i].buckets, filtered.buckets)
		}
		count := filtered.ValuesCount()
		if testData[i].count != count {
			t.Fatalf("expected %f but found %f", testData[i].count, count)
		}
	}
}
