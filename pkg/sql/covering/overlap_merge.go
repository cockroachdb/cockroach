// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package covering

import (
	"bytes"
	"reflect"
	"sort"
)

// Range is an interval with a payload.
type Range struct {
	Start   []byte
	End     []byte
	Payload interface{}
}

// intervals represent intervals of points sorted
// on one line
type intervals [][]byte

var _ sort.Interface = intervals{}

func (ip intervals) Len() int {
	return len(ip)
}

func (ip intervals) Less(i, j int) bool {
	return bytes.Compare(ip[i], ip[j]) < 0
}

func (ip intervals) Swap(i, j int) {
	ip[i], ip[j] = ip[j], ip[i]
}

// marker is a wrapper for payload and cover index in order to be able to
// append payloads based on the order among different covers
type marker struct {
	Payload       interface{}
	CoveringIndex int
}

type markers []marker

var _ sort.Interface = markers{}

func (m markers) Len() int {
	return len(m)
}

func (m markers) Less(i, j int) bool {
	// sort markers based on the cover index it appears
	return m[i].CoveringIndex < m[j].CoveringIndex
}

func (m markers) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

// Covering represents a non-overlapping, but possibly non-contiguous, set of
// intervals.
type Covering []Range

// OverlapCoveringMerge returns the set of intervals covering every range in the
// input such that no output range crosses an input endpoint. The payloads are
// returned as a `[]interface{}` and in the same order as they are in coverings.
//
// Example:
//  covering 1: [1, 2) -> 'a', [3, 4) -> 'b', [6, 7) -> 'c'
//  covering 2: [1, 5) -> 'd'
//  output: [1, 2) -> 'ad', [2, 3) -> `d`, [3, 4) -> 'bd', [4, 5) -> 'd', [6, 7) -> 'c'
//
// The input is mutated (sorted). It is also assumed (and not checked) to be
// valid (e.g. non-overlapping intervals in each covering).
func OverlapCoveringMerge(coverings []Covering) []Range {

	// TODO(dan): Verify that the ranges in each covering are non-overlapping.

	// We would like to flatten all coverings on the the single line next we sort it
	// (which takes O(n*log(n))) time, we only need to remember which points are starting
	// point of the interval and which are the end points. In addition we need to recognize
	// empty sets that also need to be counted within resulted range. Once all intervals
	// are flatten and sorted with iterate over to construct relevant ranges, iteration
	// takes O(n) time hence total complexity now is O(n*log(n)).

	var totalRange intervals
	numsMap := map[string]struct{}{}
	emptySets := map[string]struct{}{}
	startKeys := map[string]markers{}
	endKeys := map[string]markers{}

	for i, covering := range coverings {
		for _, r := range covering {
			startKeys[string(r.Start)] = append(startKeys[string(r.Start)], marker{
				Payload:       r.Payload,
				CoveringIndex: i,
			})
			if _, exist := numsMap[string(r.Start)]; !exist {
				totalRange = append(totalRange, r.Start)
				numsMap[string(r.Start)] = struct{}{}
			}
			endKeys[string(r.End)] = append(endKeys[string(r.End)], marker{
				Payload:       r.Payload,
				CoveringIndex: i,
			})

			if _, exist := numsMap[string(r.End)]; !exist {
				totalRange = append(totalRange, r.End)
				numsMap[string(r.End)] = struct{}{}
			}

			// if start and end differs then it's normal interval and
			// we can continue to the next one
			if !bytes.Equal(r.Start, r.End) {
				continue
			}
			// otherwise it is an empty interval and we need to remember it
			if _, exists := emptySets[string(r.Start)]; !exists {
				totalRange = append(totalRange, r.End)
				emptySets[string(r.Start)] = struct{}{}
			}
		}
	}
	sort.Sort(totalRange)

	var prev []byte
	var payloadsMarkes markers
	var ret []Range

	for _, next := range totalRange {
		if len(prev) != 0 && len(payloadsMarkes) > 0 {
			var payloads []interface{}
			// make sure we preserve order of covers as we got them
			sort.Sort(payloadsMarkes)

			for _, marker := range payloadsMarkes {
				payloads = append(payloads, marker.Payload)
			}

			ret = append(ret, Range{
				Start:   prev,
				End:     next,
				Payload: payloads,
			})
		}

		if removeMarkers, ok := endKeys[string(next)]; ok {
			for _, marker := range removeMarkers {
				var index = -1
				for i, p := range payloadsMarkes {
					if reflect.DeepEqual(p.Payload, marker.Payload) {
						index = i
						break
					}
				}
				if index != -1 { // if found remove
					payloadsMarkes = append(payloadsMarkes[:index], payloadsMarkes[index+1:]...)
				}
			}
		}

		// we hit empty set, no need to add anything, since it was
		// already added during previous iteration
		if bytes.Equal(prev, next) {
			continue
		}

		if addMarkers, ok := startKeys[string(next)]; ok {
			payloadsMarkes = append(payloadsMarkes, addMarkers...)
		}

		prev = next
	}

	return ret
}
