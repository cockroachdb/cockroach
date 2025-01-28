// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package benchmark

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// ScatterPlot is a helper for generating scatter plots of benchmark iteration
// durations. It writes the time taken for each bucket of iterations to a file.
// The file format is: `<iteration> <time in nanoseconds>`. Multiple runs are
// appended to the same file. The file can be plotted using a tool like gnuplot.
type ScatterPlot struct {
	timeOffsets []time.Time
	bucketSize  int
	bucketMod   int
	index       int
	startTime   time.Time
	configured  bool
}

var scatterPlotConfig struct {
	path       *string
	bucketSize *int
}

// RegisterScatterPlotFlags adds flags to the test binary necessary to enable
// scatter plot output. Add this to the top of the `TestMain` function.
func RegisterScatterPlotFlags() {
	scatterPlotConfig.path = flag.String("scatterplot", "", "output scatterplot to a file")
	scatterPlotConfig.bucketSize = flag.Int("scatterplot-bucket", 10, "number of iterations to bucket per point")
}

// NewScatterPlot initializes a ScatterPlot instance. If the flags are not set,
// the methods on the returned instance will be no-ops. This helps simplify the
// benchmark code.
func NewScatterPlot(b *testing.B) *ScatterPlot {
	if b.N <= 1 || *scatterPlotConfig.path == "" {
		return &ScatterPlot{}
	}
	if *scatterPlotConfig.bucketSize <= 0 {
		b.Fatalf("bucket size must be greater than 0")
	}
	return &ScatterPlot{
		timeOffsets: make([]time.Time, b.N/(*scatterPlotConfig.bucketSize)),
		bucketSize:  *scatterPlotConfig.bucketSize,
		bucketMod:   *scatterPlotConfig.bucketSize - 1,
		configured:  true,
	}
}

// Start starts the timer for the scatter plot. This should be called right
// before the benchmark starts.
func (sp *ScatterPlot) Start() {
	if !sp.configured {
		return
	}
	sp.startTime = timeutil.Now()
}

// Stop appends the time taken for each bucket of iterations to the output file.
// This function should be deferred so that it executes after the benchmark
// completes.
func (sp *ScatterPlot) Stop(b *testing.B) {
	if !sp.configured || sp.index == 0 {
		return
	}
	// The output is opened in append mode to allow for multiple runs to be appended to the same file.
	file, err := os.OpenFile(*scatterPlotConfig.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(b, err)
	defer func() {
		require.NoError(b, file.Close())
	}()
	prevTime := sp.startTime
	for i := 0; i < len(sp.timeOffsets); i++ {
		_, err = fmt.Fprintf(file, "%d %d\n", i, sp.timeOffsets[i].Sub(prevTime).Nanoseconds())
		require.NoError(b, err)
		prevTime = sp.timeOffsets[i]
	}
}

// Measure records the time at the given index. The call should be placed at the
// tail end of the benchmark iteration that is being measured and the index set
// to the current iteration of the benchmark.
func (sp *ScatterPlot) Measure(i int) {
	if !sp.configured {
		return
	}
	if i%sp.bucketSize == sp.bucketMod {
		sp.timeOffsets[sp.index] = timeutil.Now()
		sp.index++
	}
}
