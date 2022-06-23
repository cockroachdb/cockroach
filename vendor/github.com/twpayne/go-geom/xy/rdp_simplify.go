package xy

// Adopted from https://github.com/paulmach/orb/blob/master/simplify/douglas_peucker.go
// Original licence:
//
// The MIT License (MIT)
// Copyright (c) 2017 Paul Mach Permission is hereby granted, free of charge, to
// any person obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without restriction,
// including without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the following
// conditions: The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software. THE SOFTWARE
// IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
// INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

// SimplifyFlatCoords uses the Douglas-Peucker algorithm to simplify a 2D
// flatCoords. It returns the indexes of the points. Note that the indexes are
// based on points, So acesss to x, y pair should be:
//
//  x := flatCoords[i*stride]
//  y := flatCoords[i*stride+1]
//
// Threshold is the distance between a point and the selected start
// and end line segment. It returns the indexes of the points.
func SimplifyFlatCoords(flatCoords []float64, threshold float64, stride int) []int {
	size := len(flatCoords) / stride
	if size < 3 {
		ret := make([]int, size)
		for i := 0; i < size; i++ {
			ret[i] = i
		}
		return ret
	}
	mask := make([]byte, size)
	// first and last always added
	mask[0] = 1
	mask[len(mask)-1] = 1

	found := dpWorker(flatCoords, threshold, mask, stride)
	indexMap := make([]int, 0, found)

	for i, v := range mask {
		if v == 1 {
			indexMap = append(indexMap, i)
		}
	}

	return indexMap
}

// dpWorker does the recursive threshold checks.
// Using a stack array with a stackLength variable resulted in
// 4x speed improvement over calling the function recursively.
func dpWorker(ls []float64, threshold float64, mask []byte, stride int) int {
	found := 2

	var stack []int
	stack = append(stack, 0, len(ls)/stride-1)

	l := len(stack)
	for l > 0 {
		start := stack[l-2]
		end := stack[l-1]

		maxDist := 0.0
		maxIndex := 0
		a := ls[start*stride : start*stride+stride]
		b := ls[end*stride : end*stride+stride]

		for i := start + 1; i < end; i++ {
			p := ls[i*stride : i*stride+stride]
			dist := distanceFromSegmentSquared(a, b, p)
			if dist > maxDist {
				maxDist = dist
				maxIndex = i
			}
		}

		if maxDist > threshold*threshold {
			found++
			mask[maxIndex] = 1

			stack[l-1] = maxIndex
			stack = append(stack, maxIndex, end)
		} else {
			stack = stack[:l-2]
		}
		l = len(stack)
	}

	return found
}

// distanceFromSegmentSquared returns point's squared distance from the segment [a, b].
func distanceFromSegmentSquared(a, b, point []float64) float64 {
	x := a[0]
	y := a[1]
	dx := b[0] - x
	dy := b[1] - y

	if dx != 0 || dy != 0 {
		t := ((point[0]-x)*dx + (point[1]-y)*dy) / (dx*dx + dy*dy)

		if t > 1 {
			x = b[0]
			y = b[1]
		} else if t > 0 {
			x += dx * t
			y += dy * t
		}
	}

	dx = point[0] - x
	dy = point[1] - y

	return dx*dx + dy*dy
}
