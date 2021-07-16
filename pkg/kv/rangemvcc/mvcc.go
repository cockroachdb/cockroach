// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangemvcc

type version struct {
	ts int
	v  string
}

type rangeVersion struct {
	ts       int
	revertTo int // if 0, deletion
}

type mvccHistory struct {
	vs  []version      // ascending ts order
	rvs []rangeVersion // ascending ts order

	seq int // timestamp allocator for write ops
}

func (h *mvccHistory) Get(ts int) version {
	vs := h.vs
	rvs := h.rvs
	for {
		if len(vs) == 0 {
			return version{} // not found
		}
		var vIdx int // -1 if no version visible
		for i := len(vs) - 1; i >= -1; i-- {
			if i >= 0 && vs[i].ts > ts {
				continue // not visible
			}
			vIdx = i
			break
		}

		if vIdx < 0 {
			// No suitable version to return exists at all, so no reversion could come
			// up with any visible version neither. Return empty-handed.
			return version{}
		}

		var rIdx int
		for i := len(rvs) - 1; i >= -1; i-- {
			if i >= 0 && rvs[i].ts > ts {
				continue // not visible
			}
			rIdx = i
			break
		}

		if rIdx < 0 || rvs[rIdx].ts < vs[vIdx].ts {
			// No ranged op affects the current candidate version, so
			// we can return it.
			return vs[vIdx]
		}
		// We're getting redirected by a range op, either to timestamp
		// zero (i.e. a deletion) or to a nonzero timestamp. It's all
		// the same, just update the timestamp and loop around.
		ts = rvs[rIdx].revertTo

		// Any version that was in the future now will be in the future forever, so
		// help out the next iteration of this loop by making them disappear. Note
		// that the current version might still be returned in a future iteration of
		// the loop, for example if we're initially reading at ts=5, there is a
		// version at ts=3, and there is a reversion that redirects ts=5 to ts=3. In
		// that case, we'll see the version at ts=3 in the first loop reading at
		// ts=5, loop around to handle the revert range, and then see it again at
		// ts=3 and return it.
		vs = vs[:vIdx+1]
		// Optimize by removing range ops that will never again be visible. This
		// notably includes the current range op, since it will redirect us to a
		// timestamp at which it is no longer visible.
		rvs = rvs[:rIdx]
	}
}

func (h *mvccHistory) allocTS() int {
	h.seq++
	return h.seq
}

func (h *mvccHistory) Revert(to int) int {
	h.rvs = append(h.rvs, rangeVersion{ts: h.allocTS(), revertTo: to})
	return h.seq
}

func (h *mvccHistory) Put(v string) int {
	h.vs = append(h.vs, version{ts: h.allocTS(), v: v})
	return h.seq
}
