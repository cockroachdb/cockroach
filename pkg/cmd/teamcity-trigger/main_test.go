// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetMaxTime(t *testing.T) {
	customTimeouts = map[string]timeoutSpec{
		"//pkg/sql/logictest": {
			maxTime: 3 * time.Hour,
		},
		"//pkg/kv/kvserver": {
			maxTime: 3 * time.Hour,
		},
		"//pkg/ccl/backupccl": {
			maxTime: 2 * time.Hour,
		},
		"//pkg/ccl/logictestccl/tests/3node-tenant": {
			maxTime: 2 * time.Hour,
		},
		"//some/recursive/path": {
			maxTime:   2 * time.Hour,
			recursive: true,
		},
		"//not/recursive/path": {
			maxTime: 5 * time.Hour,
		},
	}
	for _, tc := range []struct {
		in  string
		out time.Duration
	}{
		{
			in:  "//pkg/sql/logictest:logictest_test",
			out: 3 * time.Hour,
		},
		{
			in:  "//pkg/ccl/backupccl:backupccl_test",
			out: 2 * time.Hour,
		},
		{
			in:  "//some/recursive/path/suffix/another-suffix:another-suffix_test",
			out: 2 * time.Hour,
		},
		{
			in:  "//not/in/custom/timeouts:timeouts_test",
			out: 1 * time.Hour,
		},
		{
			in:  "//not/recursive/path/suffix:suffix_test",
			out: 1 * time.Hour,
		},
	} {
		require.Equal(t, tc.out, getMaxTime(tc.in))
	}
}
