// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package httputil

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	desc               string
	path               string
	ifNoneMatch        string
	expectedStatusCode int
}

func TestEtagHandler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("(http response body)"))
		require.NoError(t, err, "HTTP handler that always returns 200 failed to write response. Something's very wrong.")
	})

	// The hashes here aren't significant, as long as they're sent in client requests
	hashedFiles := map[string]string{
		"/dist/hello.js":         "0123111",
		"/lorem/ipsum/dolor.png": "4567222",
		"/README.md":             "789afff",
	}

	handler := EtagHandler(hashedFiles, okHandler)

	cases := []testCase{
		{
			desc:               "matching ETag",
			path:               "/README.md",
			ifNoneMatch:        `"789afff"`,
			expectedStatusCode: 304,
		},
		{
			desc:               "matching but malformed ETag (missing quotes)",
			path:               "/README.md",
			ifNoneMatch:        `789afff`, // Note: no doublequotes around this hash!
			expectedStatusCode: 200,
		},
		{
			desc:               "mismatched ETag",
			path:               "/README.md",
			ifNoneMatch:        `"not the right etag"`,
			expectedStatusCode: 200,
		},
		{
			desc:               "no ETag",
			path:               "/README.md",
			expectedStatusCode: 200,
		},
		{
			desc:               "unhashed file",
			path:               "/this/file/isnt/hashed.css",
			ifNoneMatch:        `"5ca1eab1e"`,
			expectedStatusCode: 200,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(fmt.Sprintf("request to %s with %s", tc.path, tc.desc), func(t *testing.T) {
			req := httptest.NewRequest("GET", tc.path, nil)
			req.Header.Set("If-None-Match", tc.ifNoneMatch)
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)
			res := w.Result()

			defer res.Body.Close()
			require.Equal(t, tc.expectedStatusCode, res.StatusCode)

			checksum, checksumExists := hashedFiles[tc.path]
			// Requests for files with ETags must always include the ETag in the response
			if checksumExists {
				require.Equal(
					t,
					`"`+checksum+`"`,
					res.Header.Get("ETag"),
					"Requests for hashed files must always include an ETag response header",
				)
			}

			bodyBytes, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			body := string(bodyBytes)

			if tc.expectedStatusCode == 304 {
				require.Empty(t, body)
			} else if tc.expectedStatusCode == 200 {
				require.Equal(t, "(http response body)", body)
			}
		})
	}
}
