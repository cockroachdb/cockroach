package asciigraph

import (
	"bytes"
	"fmt"
	"math"
	"strings"
)

// Plot returns ascii graph for a series.
func Plot(series []float64, options ...Option) string {
	return PlotMany([][]float64{series}, options...)
}

// PlotMany returns ascii graph for multiple series.
func PlotMany(data [][]float64, options ...Option) string {
	var logMaximum float64
	config := configure(config{
		Offset:    3,
		Precision: 2,
	}, options)

	lenMax := 0
	for i := range data {
		if l := len(data[i]); l > lenMax {
			lenMax = l
		}
	}

	if config.Width > 0 {
		for i := range data {
			for j := len(data[i]); j < lenMax; j++ {
				data[i] = append(data[i], math.NaN())
			}
			data[i] = interpolateArray(data[i], config.Width)
		}

		lenMax = config.Width
	}

	minimum, maximum := math.Inf(1), math.Inf(-1)
	for i := range data {
		min, max := minMaxFloat64Slice(data[i])
		if min < minimum {
			minimum = min
		}
		if max > maximum {
			maximum = max
		}
	}
	interval := math.Abs(maximum - minimum)

	if config.Height <= 0 {
		if int(interval) <= 0 {
			config.Height = int(interval * math.Pow10(int(math.Ceil(-math.Log10(interval)))))
		} else {
			config.Height = int(interval)
		}
	}

	if config.Offset <= 0 {
		config.Offset = 3
	}

	var ratio float64
	if interval != 0 {
		ratio = float64(config.Height) / interval
	} else {
		ratio = 1
	}
	min2 := round(minimum * ratio)
	max2 := round(maximum * ratio)

	intmin2 := int(min2)
	intmax2 := int(max2)

	rows := int(math.Abs(float64(intmax2 - intmin2)))
	width := lenMax + config.Offset

	type cell struct {
		Text  string
		Color AnsiColor
	}
	plot := make([][]cell, rows+1)

	// initialise empty 2D grid
	for i := 0; i < rows+1; i++ {
		line := make([]cell, width)
		for j := 0; j < width; j++ {
			line[j].Text = " "
			line[j].Color = Default
		}
		plot[i] = line
	}

	precision := config.Precision
	logMaximum = math.Log10(math.Max(math.Abs(maximum), math.Abs(minimum))) //to find number of zeros after decimal
	if minimum == float64(0) && maximum == float64(0) {
		logMaximum = float64(-1)
	}

	if logMaximum < 0 {
		// negative log
		if math.Mod(logMaximum, 1) != 0 {
			// non-zero digits after decimal
			precision += uint(math.Abs(logMaximum))
		} else {
			precision += uint(math.Abs(logMaximum) - 1.0)
		}
	} else if logMaximum > 2 {
		precision = 0
	}

	maxNumLength := len(fmt.Sprintf("%0.*f", precision, maximum))
	minNumLength := len(fmt.Sprintf("%0.*f", precision, minimum))
	maxWidth := int(math.Max(float64(maxNumLength), float64(minNumLength)))

	// axis and labels
	for y := intmin2; y < intmax2+1; y++ {
		var magnitude float64
		if rows > 0 {
			magnitude = maximum - (float64(y-intmin2) * interval / float64(rows))
		} else {
			magnitude = float64(y)
		}

		label := fmt.Sprintf("%*.*f", maxWidth+1, precision, magnitude)
		w := y - intmin2
		h := int(math.Max(float64(config.Offset)-float64(len(label)), 0))

		plot[w][h].Text = label
		plot[w][h].Color = config.LabelColor
		plot[w][config.Offset-1].Text = "┤"
		plot[w][config.Offset-1].Color = config.AxisColor
	}

	for i := range data {
		series := data[i]

		color := Default
		if i < len(config.SeriesColors) {
			color = config.SeriesColors[i]
		}

		var y0, y1 int

		if !math.IsNaN(series[0]) {
			y0 = int(round(series[0]*ratio) - min2)
			plot[rows-y0][config.Offset-1].Text = "┼" // first value
			plot[rows-y0][config.Offset-1].Color = config.AxisColor
		}

		for x := 0; x < len(series)-1; x++ { // plot the line
			d0 := series[x]
			d1 := series[x+1]

			if math.IsNaN(d0) && math.IsNaN(d1) {
				continue
			}

			if math.IsNaN(d1) && !math.IsNaN(d0) {
				y0 = int(round(d0*ratio) - float64(intmin2))
				plot[rows-y0][x+config.Offset].Text = "╴"
				plot[rows-y0][x+config.Offset].Color = color
				continue
			}

			if math.IsNaN(d0) && !math.IsNaN(d1) {
				y1 = int(round(d1*ratio) - float64(intmin2))
				plot[rows-y1][x+config.Offset].Text = "╶"
				plot[rows-y1][x+config.Offset].Color = color
				continue
			}

			y0 = int(round(d0*ratio) - float64(intmin2))
			y1 = int(round(d1*ratio) - float64(intmin2))

			if y0 == y1 {
				plot[rows-y0][x+config.Offset].Text = "─"
			} else {
				if y0 > y1 {
					plot[rows-y1][x+config.Offset].Text = "╰"
					plot[rows-y0][x+config.Offset].Text = "╮"
				} else {
					plot[rows-y1][x+config.Offset].Text = "╭"
					plot[rows-y0][x+config.Offset].Text = "╯"
				}

				start := int(math.Min(float64(y0), float64(y1))) + 1
				end := int(math.Max(float64(y0), float64(y1)))
				for y := start; y < end; y++ {
					plot[rows-y][x+config.Offset].Text = "│"
				}
			}

			start := int(math.Min(float64(y0), float64(y1)))
			end := int(math.Max(float64(y0), float64(y1)))
			for y := start; y <= end; y++ {
				plot[rows-y][x+config.Offset].Color = color
			}
		}
	}

	// join columns
	var lines bytes.Buffer
	for h, horizontal := range plot {
		if h != 0 {
			lines.WriteRune('\n')
		}

		// remove trailing spaces
		lastCharIndex := 0
		for i := width - 1; i >= 0; i-- {
			if horizontal[i].Text != " " {
				lastCharIndex = i
				break
			}
		}

		c := Default
		for _, v := range horizontal[:lastCharIndex+1] {
			if v.Color != c {
				c = v.Color
				lines.WriteString(c.String())
			}

			lines.WriteString(v.Text)
		}
		if c != Default {
			lines.WriteString(Default.String())
		}
	}

	// add caption if not empty
	if config.Caption != "" {
		lines.WriteRune('\n')
		lines.WriteString(strings.Repeat(" ", config.Offset+maxWidth))
		if len(config.Caption) < lenMax {
			lines.WriteString(strings.Repeat(" ", (lenMax-len(config.Caption))/2))
		}
		if config.CaptionColor != Default {
			lines.WriteString(config.CaptionColor.String())
		}
		lines.WriteString(config.Caption)
		if config.CaptionColor != Default {
			lines.WriteString(Default.String())
		}
	}

	return lines.String()
}
