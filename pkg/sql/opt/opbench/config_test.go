// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opbench

import (
	"reflect"
	"testing"
)

func TestConfigIterator(t *testing.T) {
	check := func(cs []Options, expected []string) {
		ci := NewConfigIterator(&Spec{
			Inputs: cs,
		})

		var res []string
		c, ok := ci.Next()
		for ok {
			res = append(res, c.String())
			c, ok = ci.Next()
		}

		if !reflect.DeepEqual(res, expected) {
			t.Fatalf("expected %#v, got %#v", expected, res)
		}
	}

	check([]Options{
		{"a", []float64{1, 2, 3}},
		{"b", []float64{4, 5}},
	}, []string{
		"a=1/b=4",
		"a=2/b=4",
		"a=3/b=4",
		"a=1/b=5",
		"a=2/b=5",
		"a=3/b=5",
	})

	check([]Options{}, []string{""})

	check([]Options{
		{"a", []float64{1}},
		{"b", []float64{10}},
	}, []string{"a=1/b=10"})
}
