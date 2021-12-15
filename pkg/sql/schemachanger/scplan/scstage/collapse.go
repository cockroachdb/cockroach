// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scstage

// CollapseStages collapses stages with no ops together whenever possible.
func CollapseStages(stages []Stage) (collapsedStages []Stage) {
	if len(stages) <= 1 {
		return stages
	}

	accumulator := stages[0]
	for _, s := range stages[1:] {
		if isCollapsible(accumulator, s) {
			accumulator.After = s.After
			accumulator.Revertible = s.Revertible
			if s.Ops != nil {
				accumulator.Ops = s.Ops
			} else if accumulator.Ops == nil {
				panic("missing ops")
			}
		} else {
			collapsedStages = append(collapsedStages, accumulator)
			accumulator = s
		}
	}
	collapsedStages = append(collapsedStages, accumulator)
	return decorateStages(collapsedStages)
}

// isCollapsible returns true iff s can be collapsed into acc. The collapsed
// state will have:
//   - the Before state from acc,
//   - the After state from s,
//   - the Revertible flag from s.
//
// The collapse rules are:
// 1. Both states must be in the same phase.
// 2. If acc doesn't have ops and s does, we can collapse acc and s together.
//    This means we lose the ability to revert the acc state, but since it
//    doesn't have any ops this effectively doesn't matter.
// 3. If s doesn't have ops, then it can be collapsed into acc. However,
//    unlike the previous case, assuming acc is revertible, we don't want to
//    lose that property when s isn't revertible.
func isCollapsible(acc Stage, s Stage) bool {
	if acc.Phase != s.Phase {
		return false
	}
	// At this point acc and s are in the same phase, we can consider collapsing.
	if acc.Ops == nil {
		return true
	}
	if s.Ops != nil {
		return false
	}
	// At this point, acc has ops, s has none, and they are in the same phase.
	// From now on, collapsibility will be determined by revertibility.
	if !acc.Revertible {
		// If acc is not revertible, then nothing after can be anyway.
		return true
	}
	// If acc is revertible, only collapse no-op stage s into it if it's also
	// revertible, otherwise acc will be made non-revertible. We don't want that
	// because it has ops and s doesn't.
	return s.Revertible
}
