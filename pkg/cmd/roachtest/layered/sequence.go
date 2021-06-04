// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package layered

import "math/rand"

// A Sequence is a number of Steps to be executed one after another. The
// sequence may continue forever, or it may stop by returning `false`. The
// caller will only request a next element when the previous element succeeded,
// i.e. each Step can explicitly rely on seeing the cumulative results of all
// previous steps.
type Sequence interface {
	String() string
	Next(r *rand.Rand) (Step, bool)
}

// SliceSequence is a Sequence that iterates through a slice of Steps.
type SliceSequence struct {
	Name  string
	Idx   int
	Items []Step
}

// Next implements Sequence.
func (s *SliceSequence) Next(r *rand.Rand) (Step, bool) {
	if s.Idx >= len(s.Items) {
		return nil, false
	}
	step := s.Items[s.Idx]
	s.Idx++
	return step, true
}

func (s *SliceSequence) String() string {
	return s.Name
}
