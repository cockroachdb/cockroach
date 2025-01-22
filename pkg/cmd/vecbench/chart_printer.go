// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"math"
	"strings"

	"github.com/guptarohit/asciigraph"
	"golang.org/x/term"
)

// chartData contains data for a single time series chart.
type chartData struct {
	caption string
	x       int
	y       int
	width   int
	height  int

	series []float64
	start  int
	end    int
}

// chartPrinter display a grid of ASCII time series charts in the console,
// similar to this:
//
// 1722 ┤    ╭─╮                ╭─────╮╭   2.00 ┼──╮      ╭╮  ╭─╮        ╭╮
// 1700 ┤╭╮  │ ╰╮   ╭╮         ╭╯     ╰╯   1.60 ┤  │      ││  │ │        ││
// 1678 ┤││ ╭╯  ╰╮ ╭╯╰╮╭─╮╭╮ ╭─╯           1.20 ┤  ╰───╮╭─╯╰──╯ ╰─╮╭─────╯╰─────
// 1656 ┼╯│╭╯    │╭╯  ╰╯ ╰╯╰─╯             0.80 ┤      ││         ││
// 1634 ┤ ││     ╰╯                        0.40 ┤      ││         ││
// 1612 ┤ ╰╯                               0.00 ┤      ╰╯         ╰╯
//
//	ops/sec (1707.98)                   fixup queue size (1.00)
//
// 0.68 ┼─╮                                55.10 ┤                ╭╮
// 0.64 ┤ ╰─╮                              54.40 ┤            ╭╮ ╭╯╰─╮ ╭──╮
// 0.59 ┤   ╰╮                             53.70 ┤            │╰─╯   ╰─╯  ╰╮
// 0.54 ┤    ╰╮                            53.00 ┤            │            ╰────
// 0.50 ┤     ╰╮                           52.30 ┤      ╭╮ ╭──╯
// 0.45 ┤      ╰──────────╮                51.60 ┼─╮╭───╯╰─╯
// 0.41 ┤                 ╰─────────────   50.90 ┤ ╰╯
//
//	p50 ms latency (0.41)                   p99 ms latency (53.15)
//
// Example usage:
//
//	var cp chartPrinter
//	series1 := cp.AddChart("series 1")
//	series2 := cp.AddChart("series 2")
//	for {
//		...
//		cp.AddSample(series1, sample1)
//		cp.AddSample(series2, sample2)
//		cp.Plot()
//		...
//	}
type chartPrinter struct {
	// Footer is the number of rows to reserve below the charts.
	Footer int
	// Don't show the chart. This will be automatically set if the console size
	// cannot be determined, e.g. because we're running under a debugger.
	Hide bool

	consoleWidth  int
	consoleHeight int
	charts        []chartData
	cleared       bool
}

// AddChart adds a new chart with the given caption to the chart printer. It
// returns the ID of the chart to be used when calling AddSample.
// NOTE: When a new chart is added, all existing charts have any existing
// samples cleared.
func (cp *chartPrinter) AddChart(caption string) int {
	if len(cp.charts) == 0 {
		// Get size of console window. File descriptor 0 is stdin.
		var err error
		cp.consoleWidth, cp.consoleHeight, err = term.GetSize(0)
		if err != nil {
			// Terminal does not support size, e.g. because we're running under a
			// debugger. In this case, hide the charts.
			cp.Hide = true
			cp.consoleWidth = 80
			cp.consoleHeight = 40
		}
		cp.consoleHeight -= cp.Footer
	}

	// Add new chart, but preserve captions for existing charts.
	numCharts := len(cp.charts) + 1
	newCharts := make([]chartData, numCharts)
	copy(newCharts, cp.charts)
	newCharts[numCharts-1].caption = caption
	cp.charts = newCharts

	// Re-initialize all previous charts, taking into account new chart. Divide
	// the charts into a square grid.
	grid := math.Ceil(math.Sqrt(float64(numCharts)))
	xInc := float64(cp.consoleWidth) / grid
	yInc := float64(cp.consoleHeight) / grid

	x := 0.0
	y := 0.0
	for i := range cp.charts {
		nextX := x + xInc
		chartWidth := int(nextX) - int(x) - 8
		nextY := y + yInc
		chartHeight := int(nextY) - int(y) - 2

		cp.charts[i].x = int(x)
		cp.charts[i].y = int(y)
		cp.charts[i].width = chartWidth
		cp.charts[i].height = chartHeight
		cp.charts[i].series = make([]float64, chartWidth*2)
		cp.charts[i].start = 0
		cp.charts[i].end = 0

		x = nextX
		if int(x+1) >= cp.consoleWidth {
			x = 0
			y = nextY
		}
	}

	return numCharts - 1
}

// AddSample adds a new sample for the given chart. The chart ID should have
// been obtained via a call to AddChart.
func (cp *chartPrinter) AddSample(chartID int, sample float64) {
	chart := &cp.charts[chartID]

	if chart.end-chart.start < chart.width {
		chart.series[chart.end] = sample
		chart.end++
	} else {
		if chart.start >= chart.width {
			chart.start = 0
			chart.end = chart.width
		}
		chart.series[chart.start] = sample
		chart.start++
		chart.series[chart.end] = sample
		chart.end++
	}
}

// Plot prints the most recent samples for the charts on the console.
func (cp *chartPrinter) Plot() {
	if cp.Hide {
		return
	}

	// Clear the console once. This "scrolls up" history so that it's not
	// overwritten. After that, clear the console by writing spaces to it, since
	// we don't want to keep chart printing history.
	if !cp.cleared {
		asciigraph.Clear()
		cp.cleared = true
	} else {
		for y := range cp.consoleHeight {
			blanks := strings.Repeat(" ", cp.consoleWidth)
			cp.printAt(0, y, blanks)
		}
	}

	// Print charts.
	for i := range cp.charts {
		chart := &cp.charts[i]

		plotStr := asciigraph.Plot(chart.series[chart.start:chart.end],
			asciigraph.Width(min(chart.end-chart.start, chart.width)),
			asciigraph.Height(chart.height))

		// Print each line at specific location.
		lines := strings.Split(plotStr, "\n")
		if len(lines) < chart.height {
			// Vertically pad the chart.
			plotStr = strings.Repeat("\n", chart.height-len(lines)) + plotStr
			lines = strings.Split(plotStr, "\n")
		}
		for i, line := range lines {
			cp.printAt(chart.x, chart.y+i, line)
		}

		// Print caption.
		x := chart.x + (chart.width-len(chart.caption))/2
		y := chart.y + len(lines)
		cp.printAt(x, y, "%s (%0.2f)", White+chart.caption+Reset, chart.series[chart.end-1])
	}

	// Move cursor to footer.
	lastChart := &cp.charts[len(cp.charts)-1]
	cp.printAt(0, lastChart.y+lastChart.height+3, "")
}

func (cp *chartPrinter) printAt(x, y int, format string, args ...any) {
	// Console positions are 1-based.
	fmt.Printf("\033[%d;%dH", y+1, x+1)
	fmt.Printf(format, args...)
}
