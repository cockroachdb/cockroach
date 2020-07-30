// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package iterutil

import (
	"testing"

	"github.com/cockroachdb/errors"
)

func TestIter(t *testing.T) {
	input := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	var output []string
	fn := func(s *State) error {
		str, ok := s.Cur().(string)
		if !ok {
			return errors.Newf("extpected type string; got %T", s.Cur())
		}

		output = append(output, str)
		return nil
	}
	if err := sampleIter(input, fn); err != nil {
		t.Errorf("unexpected error while iterating: %v", err)
	}
	if len(input) != len(output) {
		t.Errorf("expected output length: %d; got %d", len(input), len(output))
	}
	for i := range input {
		if input[i] != output[i] {
			t.Errorf("expected output[%d]: %s; got %s", i, input[i], output[i])
		}
	}

	output = []string{}
	fn = func(s *State) error {
		str := s.Cur().(string)
		if str == "e" {
			return s.Stop()
		}
		output = append(output, str)
		return nil
	}
	if err := sampleIter(input, fn); err != nil {
		t.Errorf("unexpected error while iterating: %v", err)
	}
	if len(output) != 4 {
		t.Errorf("expected output length: 4; got %d", len(output))
	}
	for i := 0; i < 3; i++ {
		if input[i] != output[i] {
			t.Errorf("expected output[%d]: %s; got %s", i, input[i], output[i])
		}
	}
}

// sampleIter iterates over the slice and applies the closure.
func sampleIter(list []string, closure func(s *State) error) error {
	var it State
	for _, elem := range list {
		it.Update(elem)
		if err := closure(&it); err != nil {
			return err
		}
		if it.Done() {
			break
		}
	}
	return nil
}
