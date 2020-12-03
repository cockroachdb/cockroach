// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package humanizeutil

import (
	"testing"
	"time"
)

func TestDuration(t *testing.T) {
	testCases := []struct {
		val time.Duration
		exp string
	}{
		{val: 0, exp: "0µs"},
		{val: 12, exp: "0µs"},
		{val: 499, exp: "0µs"},
		{val: 500, exp: "1µs"},
		{val: 501, exp: "1µs"},
		{val: 1234, exp: "1µs"},
		{val: 1500, exp: "2µs"},
		{val: 12345, exp: "12µs"},
		{val: 123456, exp: "123µs"},
		{val: 1234567, exp: "1ms"},
		{val: 12345678, exp: "12ms"},
		{val: 123456791, exp: "123ms"},
		{val: 999999999, exp: "1s"},
		{val: 1000000001, exp: "1s"},
		{val: 1234567912, exp: "1.2s"},
		{val: 12345679123, exp: "12.3s"},
		{val: 123456791234, exp: "2m3s"},
		{val: 1234567912345, exp: "20m35s"},
		{val: 12345679123456, exp: "3h25m46s"},
		{val: 123456791234567, exp: "34h17m37s"},
		{val: 1234567912345678, exp: "342h56m8s"},
		{val: 12345679123456789, exp: "3429h21m19s"},
	}
	for _, tc := range testCases {
		t.Run(tc.val.String(), func(t *testing.T) {
			res := Duration(tc.val)
			if res != tc.exp {
				t.Errorf("expected '%s', got '%s'", tc.exp, res)
			}
		})
	}
}
