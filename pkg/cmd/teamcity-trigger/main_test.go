// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetMaxTime(t *testing.T) {
	customTimeouts = map[string]timeoutSpec{
		"./pkg/sql/logictest": {
			maxTime: 3 * time.Hour,
		},
		"./pkg/kv/kvserver": {
			maxTime: 3 * time.Hour,
		},
		"./pkg/ccl/backupccl": {
			maxTime: 2 * time.Hour,
		},
		"./pkg/ccl/logictestccl/tests/3node-tenant": {
			maxTime: 2 * time.Hour,
		},
		"./some/recursive/path": {
			maxTime:   2 * time.Hour,
			recursive: true,
		},
		"./not/recursive/path": {
			maxTime: 5 * time.Hour,
		},
	}
	for _, tc := range []struct {
		in  string
		out time.Duration
	}{
		{
			in:  "./pkg/sql/logictest",
			out: 3 * time.Hour,
		},
		{
			in:  "./pkg/ccl/backupccl",
			out: 2 * time.Hour,
		},
		{
			in:  "./some/recursive/path/suffix/another/suffix",
			out: 2 * time.Hour,
		},
		{
			in:  "./not/in/custom/timeouts",
			out: 1 * time.Hour,
		},
		{
			in:  "./not/recursive/path/suffix",
			out: 1 * time.Hour,
		},
	} {
		require.Equal(t, tc.out, getMaxTime(tc.in))
	}
}
