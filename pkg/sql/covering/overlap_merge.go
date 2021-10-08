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

// endpoints collections of all start and endpoints
type endpoints [][]byte

var _ sort.Interface = endpoints{}

func (e endpoints) Len() int {
	return len(e)
}

func (e endpoints) Less(i, j int) bool {
	return bytes.Compare(e[i], e[j]) < 0
}

func (e endpoints) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// marker is a wrapper for payload and cover index in order to be able to
// append payloads based on the order among different covers
type marker struct {
	payload       interface{}
	coveringIndex int
}

type markers []marker

var _ sort.Interface = markers{}

func (m markers) Len() int {
	return len(m)
}

func (m markers) Less(i, j int) bool {
	// sort markers based on the cover index it appears
	return m[i].coveringIndex < m[j].coveringIndex
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

	// We would like to flatten all coverings on the single line. Assume that total
	// amount of coverings is N, then overall there are 2*N endpoints. Flatten coverings
	// on single line and sort them will take O(NlogN) time. Later we pass over endpoints
	// one by one and append payloads.

	// captures all endpoints (starts and ends from) collected from all coverings
	var totalRange endpoints
	//
	numsMap := map[string]struct{}{}
	// map to store an empty sets, need to distinguish
	// cause in such case start and end are equals, hence
	// need to prevent duplications
	emptySets := map[string]struct{}{}
	// captures all start covering's endpoints with relevant payloads
	startKeys := map[string]markers{}
	// captures all end covering's endpoints with relevant payloads
	endKeys := map[string]markers{}

	for i, covering := range coverings {
		for _, r := range covering {
			startKeys[string(r.Start)] = append(startKeys[string(r.Start)], marker{
				payload:       r.Payload,
				coveringIndex: i,
			})
			if _, exist := numsMap[string(r.Start)]; !exist {
				totalRange = append(totalRange, r.Start)
				numsMap[string(r.Start)] = struct{}{}
			}
			endKeys[string(r.End)] = append(endKeys[string(r.End)], marker{
				payload:       r.Payload,
				coveringIndex: i,
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
	var payloadsMarkers markers
	var ret []Range

	for it, next := range totalRange {
		if len(prev) != 0 && len(payloadsMarkers) > 0 {
			var payloads []interface{}
			// make sure we preserve order of covers as we got them
			sort.Sort(payloadsMarkers)

			for _, marker := range payloadsMarkers {
				payloads = append(payloads, marker.payload)
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
				for i, p := range payloadsMarkers {
					if reflect.DeepEqual(p.payload, marker.payload) {
						index = i
						break
					}
				}
				if index != -1 { // if found remove
					payloadsMarkers = append(payloadsMarkers[:index], payloadsMarkers[index+1:]...)
				}
			}
		}

		// we hit empty set, no need to add anything, since it was
		// already added during previous iteration
		if bytes.Equal(prev, next) && it > 0 {
			// the check for it > 0 needed to take care
			// of the case where start and end of an empty
			// set presented with zero length slice, since
			// this is a value prev and next both are initialized.
			continue
		}

		if addMarkers, ok := startKeys[string(next)]; ok {
			payloadsMarkers = append(payloadsMarkers, addMarkers...)
		}

		prev = next
	}

	return ret
}
