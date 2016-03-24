// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

package roachpb

// sumMultiple is the int64 number used to represent 1.0 at a scale of 3.
const sumMultiple = 1000

// Summation returns the sum value for this sample.
func (samp InternalTimeSeriesSample) Summation() float64 {
	return float64(samp.Sum) / sumMultiple
}

// Average returns the average value for this sample.
func (samp InternalTimeSeriesSample) Average() float64 {
	if samp.Count == 0 {
		return 0
	}
	return samp.Summation() / float64(samp.Count)
}

// Maximum returns the maximum value encountered by this sample.
func (samp InternalTimeSeriesSample) Maximum() float64 {
	if samp.Count < 2 {
		return samp.Summation()
	}
	return float64(*samp.Max) / sumMultiple
}

// Minimum returns the minimum value encountered by this sample.
func (samp InternalTimeSeriesSample) Minimum() float64 {
	if samp.Count < 2 {
		return samp.Summation()
	}
	return float64(*samp.Min) / sumMultiple
}

// TimeSeriesSampleSum takes a sum in float64 and converts it into
// a fixed point represented as an int64.
func TimeSeriesSampleSum(n float64) int64 {
	return int64(n * sumMultiple)
}
