// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// This file provides generic interfaces that allow tests to set up test tenants
// without importing the server package (avoiding circular dependencies). This

package serverutils

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestWithPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name        string
		testURL     TestURL
		withPath    string
		expectedStr string
	}{
		{
			name:        "append",
			testURL:     NewTestURL("http://localhost:8080"),
			withPath:    "/test",
			expectedStr: "http://localhost:8080/test",
		},
		{
			name:        "query on base url",
			testURL:     NewTestURL("http://localhost:8080/operation?cluster=1"),
			withPath:    "/test",
			expectedStr: "http://localhost:8080/operation/test?cluster=1",
		},
		{
			name:        "append path with query",
			testURL:     NewTestURL("http://localhost:8080"),
			withPath:    "/test?foo=bar",
			expectedStr: "http://localhost:8080/test?foo=bar",
		},
		{
			name:        "combine paths with queries",
			testURL:     NewTestURL("http://localhost:8080?cluster=1"),
			withPath:    "/test?foo=bar&arg=1",
			expectedStr: "http://localhost:8080/test?arg=1&cluster=1&foo=bar",
		},
		{
			name:        "escaped characters before",
			testURL:     NewTestURL("http://localhost:8080/hello%20world?cluster=1"),
			withPath:    "/test%20test?foo=bar",
			expectedStr: "http://localhost:8080/hello%20world/test%20test?cluster=1&foo=bar",
		},
		{
			name:        "escaped characters after",
			testURL:     NewTestURL("http://localhost:8080/hello world?cluster=1"),
			withPath:    "/test test?foo=bar",
			expectedStr: "http://localhost:8080/hello%20world/test%20test?cluster=1&foo=bar",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualURL := tc.testURL.WithPath(tc.withPath)
			require.Equal(t, tc.expectedStr, actualURL.String())
		})
	}
}
