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

// Summation returns the sum value for this sample.
func (samp InternalTimeSeriesSample) Summation() float64 {
	return samp.Sum
}

// Average returns the average value for this sample.
func (samp InternalTimeSeriesSample) Average() float64 {
	if samp.Count == 0 {
		return 0
	}
	return samp.Sum / float64(samp.Count)
}

// Maximum returns the maximum value encountered by this sample.
func (samp InternalTimeSeriesSample) Maximum() float64 {
	if samp.Count < 2 {
		return samp.Sum
	}
	if samp.Max != nil {
		return *samp.Max
	}
	return 0
}

// Minimum returns the minimum value encountered by this sample.
func (samp InternalTimeSeriesSample) Minimum() float64 {
	if samp.Count < 2 {
		return samp.Sum
	}
	if samp.Min != nil {
		return *samp.Min
	}
	return 0
}
