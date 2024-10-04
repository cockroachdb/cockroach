// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package httputil

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func mustParseURL(unparsed string) *url.URL {
	out, err := url.Parse(unparsed)
	if err != nil {
		panic(err)
	}

	return out
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
	server := httptest.NewServer(handler)
	defer server.Close()
	client := server.Client()

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
			tmp := mustParseURL(server.URL + tc.path)
			fmt.Printf("GETing url '%s'\n", tmp)
			resp, err := client.Do(&http.Request{
				URL: mustParseURL(server.URL + tc.path),
				Header: http.Header{
					"If-None-Match": []string{tc.ifNoneMatch},
				},
			})

			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, tc.expectedStatusCode, resp.StatusCode)

			checksum, checksumExists := hashedFiles[tc.path]
			// Requests for files with ETags must always include the ETag in the response
			if checksumExists {
				require.Equal(
					t,
					`"`+checksum+`"`,
					resp.Header.Get("ETag"),
					"Requests for hashed files must always include an ETag response header",
				)
			}

			bodyBytes, err := io.ReadAll(resp.Body)
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
