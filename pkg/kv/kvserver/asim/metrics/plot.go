// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metrics

import (
	"strings"

	"github.com/guptarohit/asciigraph"
)

var (
	// plotColors is used to determine the coloring of series graph output. It
	// is ordered in terms of visibility. We default to all colors unordered
	// when the number of series is greater than 13.
	plotColors = []asciigraph.AnsiColor{
		asciigraph.Blue,
		asciigraph.Chocolate,
		asciigraph.Ivory,
		asciigraph.Indigo,
		asciigraph.Aqua,
		asciigraph.Crimson,
		asciigraph.DarkGreen,
		asciigraph.HotPink,
		asciigraph.DarkGray,
		asciigraph.Khaki,
		asciigraph.BlanchedAlmond,
		asciigraph.DarkOrchid,
		asciigraph.ForestGreen,
	}
)

func getPlotColors(n int) []asciigraph.AnsiColor {
	ret := []asciigraph.AnsiColor{}
	if n > len(plotColors) {
		for _, color := range asciigraph.ColorNames {
			ret = append(ret, color)
		}
	} else {
		ret = plotColors[:n]
	}
	return ret
}

// PlotSeries plots the series associated with the given tags. The plotted tags
// are returned in a string, separated by a double new line.
func PlotSeries(series map[string][][]float64, tags ...string) string {
	var h, w = 20, 200
	var buf strings.Builder
	for _, tag := range tags {
		data, ok := series[tag]
		if !ok {
			continue
		}
		colors := getPlotColors(len(data))
		buf.WriteString(asciigraph.PlotMany(data, asciigraph.SeriesColors(colors...), asciigraph.Caption(tag), asciigraph.Height(h), asciigraph.Width(w)))
		buf.WriteString("\n\n")
	}
	return buf.String()
}
