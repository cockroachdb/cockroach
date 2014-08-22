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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package structured

// LatLong specifies a (latitude, longitude, altitude, accuracy)
// quadruplet with 64-bit floating point precision. Altitude and
// accuracy are in meters.
type LatLong struct {
	latitude, longitude, altitude, accuracy float64
}

// IntegerSet is a set of int64 integer values.
type IntegerSet map[int64]struct{}

// StringSet is a set of string values.
type StringSet map[string]struct{}

// IntegerMap is a map from string key to int64 integer value.
type IntegerMap map[string]int64

// StringMap is a map from string key to string value.
type StringMap map[string]string
