// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cmpconn

import (
	"testing"

	"github.com/cockroachdb/apd/v2"
)

func TestCompareVals(t *testing.T) {
	for i, tc := range []struct {
		equal bool
		a, b  []interface{}
	}{
		{
			equal: true,
			a:     []interface{}{apd.New(-2, 0)},
			b:     []interface{}{apd.New(-2, 0)},
		},
		{
			equal: true,
			a:     []interface{}{apd.New(2, 0)},
			b:     []interface{}{apd.New(2, 0)},
		},
		{
			equal: false,
			a:     []interface{}{apd.New(2, 0)},
			b:     []interface{}{apd.New(-2, 0)},
		},
		{
			equal: true,
			a:     []interface{}{apd.New(2, 0)},
			b:     []interface{}{apd.New(2000001, -6)},
		},
		{
			equal: false,
			a:     []interface{}{apd.New(2, 0)},
			b:     []interface{}{apd.New(200001, -5)},
		},
		{
			equal: false,
			a:     []interface{}{apd.New(-2, 0)},
			b:     []interface{}{apd.New(-200001, -5)},
		},
		{
			equal: false,
			a:     []interface{}{apd.New(2, 0)},
			b:     []interface{}{apd.New(-2000001, -6)},
		},
	} {
		err := CompareVals(tc.a, tc.b)
		equal := err == nil
		if equal != tc.equal {
			t.Log("test index", i)
			if err != nil {
				t.Fatal(err)
			} else {
				t.Fatal("expected unequal")
			}
		}
	}
}
