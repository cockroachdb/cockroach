// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulkpb

import (
	"fmt"
	"strings"
	"time"

	"github.com/codahale/hdrhistogram"
)

type HistogramDataType int

const (
	HistogramDataTypeLatency HistogramDataType = iota
	HistogramDataTypeBytes
)

const mb = 1 << 20

// String implements the stringer interface.
func (h *HistogramData) String() string {
	var b strings.Builder
	hist := hdrhistogram.Import(&hdrhistogram.Snapshot{
		LowestTrackableValue:  h.LowestTrackableValue,
		HighestTrackableValue: h.HighestTrackableValue,
		SignificantFigures:    h.SignificantFigures,
		Counts:                h.Counts,
	})
	switch h.DataType {
	case HistogramDataTypeLatency:
		b.WriteString(fmt.Sprintf("min: %.6f\n", float64(hist.Min())/float64(time.Second)))
		b.WriteString(fmt.Sprintf("max: %.6f\n", float64(hist.Max())/float64(time.Second)))
		b.WriteString(fmt.Sprintf("p5: %.6f\n", float64(hist.ValueAtQuantile(5))/float64(time.Second)))
		b.WriteString(fmt.Sprintf("p50: %.6f\n", float64(hist.ValueAtQuantile(50))/float64(time.Second)))
		b.WriteString(fmt.Sprintf("p90: %.6f\n", float64(hist.ValueAtQuantile(90))/float64(time.Second)))
		b.WriteString(fmt.Sprintf("p99: %.6f\n", float64(hist.ValueAtQuantile(99))/float64(time.Second)))
		b.WriteString(fmt.Sprintf("p99_9: %.6f\n", float64(hist.ValueAtQuantile(99.9))/float64(time.Second)))
		b.WriteString(fmt.Sprintf("mean: %.6f\n", float32(hist.Mean())/float32(time.Second)))
		b.WriteString(fmt.Sprintf("count: %d\n", hist.TotalCount()))
	case HistogramDataTypeBytes:
		b.WriteString(fmt.Sprintf("min: %.6f\n", float64(hist.Min())/float64(mb)))
		b.WriteString(fmt.Sprintf("max: %.6f\n", float64(hist.Max())/float64(mb)))
		b.WriteString(fmt.Sprintf("p5: %.6f\n", float64(hist.ValueAtQuantile(5))/float64(mb)))
		b.WriteString(fmt.Sprintf("p50: %.6f\n", float64(hist.ValueAtQuantile(50))/float64(mb)))
		b.WriteString(fmt.Sprintf("p90: %.6f\n", float64(hist.ValueAtQuantile(90))/float64(mb)))
		b.WriteString(fmt.Sprintf("p99: %.6f\n", float64(hist.ValueAtQuantile(99))/float64(mb)))
		b.WriteString(fmt.Sprintf("p99_9: %.6f\n", float64(hist.ValueAtQuantile(99.9))/float64(mb)))
		b.WriteString(fmt.Sprintf("mean: %.6f\n", float32(hist.Mean())/float32(mb)))
		b.WriteString(fmt.Sprintf("count: %d\n", hist.TotalCount()))
	}

	return b.String()
}
