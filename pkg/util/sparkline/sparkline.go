// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sparkline

import (
	"image"
	"image/color"

	"github.com/cockroachdb/errors"
)

// DrawSparkline draws a sparkline of the (time series) data and returns a corresponding image.
func DrawSparkline(data []float64, width, height int) (*image.RGBA, error) {
	if len(data) == 0 {
		return nil, errors.New("empty data") // No data to draw
	}

	// Create a new RGBA image
	img := image.NewRGBA(image.Rect(0, 0, width, height))

	// Set the background to transparent
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.Transparent)
		}
	}

	// Determine min and max values
	min, max := data[0], data[0]
	for _, value := range data {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}

	// Scale and draw the line
	for i := 0; i < len(data)-1; i++ {
		x1 := int(float64(i) / float64(len(data)-1) * float64(width))
		x2 := int(float64(i+1) / float64(len(data)-1) * float64(width))
		y1 := int((1 - (data[i]-min)/(max-min)) * float64(height))
		y2 := int((1 - (data[i+1]-min)/(max-min)) * float64(height))

		lineColor := color.RGBA{0, 0, 255, 255} // Blue color for the line
		drawLine(img, x1, y1, x2, y2, lineColor)
	}
	// Draw the x-axis in white.
	drawLine(img, 0, height-1, width-1, height-1, color.RGBA{255, 255, 255, 255})

	return img, nil
}

// drawLine draws a line between two points using Bresenham's line algorithm.
//
// For algorithm details, see [1] for the pseudocode, and [2] for derivation.
// [1] https://en.wikipedia.org/wiki/Bresenham%27s_line_algorithm#All_cases
// [2] https://zingl.github.io/Bresenham.pdf
func drawLine(img *image.RGBA, x1, y1, x2, y2 int, c color.Color) {
	dx := abs(x2 - x1)
	dy := abs(y2 - y1)
	sx := 1
	if x1 < x2 {
		sx = 1
	} else {
		sx = -1
	}
	sy := 1
	if y1 < y2 {
		sy = 1
	} else {
		sy = -1
	}

	err := dx - dy
	for {
		img.Set(x1, y1, c)
		if x1 == x2 && y1 == y2 {
			break
		}
		err2 := err * 2
		if err2 > -dy {
			err -= dy
			x1 += sx
		}
		if err2 < dx {
			err += dx
			y1 += sy
		}
	}
}

// abs returns the absolute value of an integer.
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
