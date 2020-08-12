// Copyright 2020 The Cockroach Authors.
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
	"github.com/stretchr/testify/require"
)

func TestIter(t *testing.T) {
	input := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	var output []string
	fn := func(c Cur) error {
		str, ok := c.Elem.(*string)
		if !ok {
			return errors.Newf("extpected type *string; got %T", c.Elem)
		}
		output = append(output, *str)
		return nil
	}
	require.NoError(t, sampleIter(input, fn))
	require.Len(t, output, len(input))
	require.ElementsMatch(t, input, output)

	output = []string{}
	stopAt := 4
	fn = func(c Cur) error {
		str := c.Elem.(*string)
		if c.Index == stopAt {
			return c.Stop()
		}
		output = append(output, *str)
		return nil
	}
	require.NoError(t, sampleIter(input, fn))
	require.Len(t, output, stopAt)
	require.ElementsMatch(t, input[:stopAt], output)
}

// sampleIter iterates over the slice and applies the closure.
func sampleIter(list []string, closure func(c Cur) error) error {
	it := NewState()
	it.Elem = new(string)
	for _, elem := range list {
		*it.Elem.(*string) = elem
		if err := closure(it.Current()); err != nil {
			return err
		}
		if it.Done() {
			break
		}
	}
	return nil
}
