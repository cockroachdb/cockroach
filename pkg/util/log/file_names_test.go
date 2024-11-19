// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestShortHostName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		input  string
		output string
	}{
		{"abc", "abc"},
		{"www.example.com", "www"},
		{"", ""},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.output, shortHostname(tc.input))
	}
}

func TestNormalizeFileName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		input                string
		outputWithHyphens    string
		outputWithoutHyphens string
	}{
		{"abc", "abc", "abc"},
		{"", "", ""},
		{"...", "", ""},
		{"www.example.com", "wwwexamplecom", "wwwexamplecom"},
		{"my-big/test", "my-bigtest", "mybigtest"},
		{"ελλάδα-☃︎..☀️", "ελλάδα-", "ελλάδα"},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.outputWithHyphens, normalizeFileName(tc.input, true))
		require.Equal(t, tc.outputWithoutHyphens, normalizeFileName(tc.input, false))
	}
}
