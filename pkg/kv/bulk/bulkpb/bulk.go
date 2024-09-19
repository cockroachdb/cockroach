// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	stringify := func(denominator float64) {
		b.WriteString(fmt.Sprintf("min: %.6f\n", float64(hist.Min())/denominator))
		b.WriteString(fmt.Sprintf("max: %.6f\n", float64(hist.Max())/denominator))
		b.WriteString(fmt.Sprintf("p5: %.6f\n", float64(hist.ValueAtQuantile(5))/denominator))
		b.WriteString(fmt.Sprintf("p50: %.6f\n", float64(hist.ValueAtQuantile(50))/denominator))
		b.WriteString(fmt.Sprintf("p90: %.6f\n", float64(hist.ValueAtQuantile(90))/denominator))
		b.WriteString(fmt.Sprintf("p99: %.6f\n", float64(hist.ValueAtQuantile(99))/denominator))
		b.WriteString(fmt.Sprintf("p99_9: %.6f\n", float64(hist.ValueAtQuantile(99.9))/denominator))
		b.WriteString(fmt.Sprintf("mean: %.6f\n", float32(hist.Mean())/float32(denominator)))
		b.WriteString(fmt.Sprintf("count: %d\n", hist.TotalCount()))
	}

	switch h.DataType {
	case HistogramDataTypeLatency:
		stringify(float64(time.Second))
	case HistogramDataTypeBytes:
		stringify(float64(mb))
	}

	return b.String()
}
