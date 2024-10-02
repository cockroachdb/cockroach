// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/assert"
)

func TestExtractPrNumbers(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			"99a4816fc2 Merge pull request #69991 from cockroachdb/blathers/backport-release-21.2-69961",
			[]string{"69991"},
		},
		{
			"478a4d8ca4 Merge #69674 #69881 #69910 #69922",
			[]string{"69674", "69881", "69910", "69922"},
		},
		{
			"c8a62f290a Merge #64957\ndiff --cc pkg/ccl/sqlproxyccl/denylist/BUILD.bazel # comment",
			[]string{"64957"},
		},
		{"FAIL", nil},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, tc.expected, extractPrNumbers(tc.input))
		})
	}
}

func TestReadToken(t *testing.T) {
	output, err := os.CreateTemp("", "token_test_file")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Remove(output.Name()); err != nil {
			t.Fatal(err)
		}
	}()

	// Writing to the test file
	text := []byte("hjk23434343j\n" + "SPACE")
	if _, err = output.Write(text); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}
	if err := output.Close(); err != nil {
		log.Fatal(err)
	}

	token, _ := readToken(output.Name())
	failToken, _ := readToken("Fail")
	dat, _ := os.ReadFile(output.Name())

	assert.Equal(t, token, string(dat))
	assert.Equal(t, failToken, "")
}

func TestFilterPullRequests(t *testing.T) {

	tests := []struct {
		input    string
		expected []string
	}{
		{`1111 Merge pull request #1111 
						 2222 Merge pull request #2222
						 3333 Merge pull request #3333
						 4444 Merge pull request #4444`, []string{"1111", "2222", "3333", "4444"}},
		{`478a4d8ca4 Merge #69674 #69881 #69910 #69922
			d855b7b5f7 Merge #69957
			1a33383ccc Merge pull request #69969 from jbowens/jackson/pebble-release-21.2-6c12d67b83e6
			99a4816fc2 Merge pull request #69991 from cockroachdb/blathers/backport-release-21.2-69961`,
			[]string{"478a4d8ca4", "d855b7b5f7", "1a33383ccc", "99a4816fc2"}},
		{`1111 Pull request #1111
						 2222 Merge request #2222
					   3333 Super pull request #3333
					   4444 Ultra pull request #4444`, []string(nil)},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, tc.expected, filterPullRequests(tc.input))
		})
	}

}

func TestMatchVersion(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// No other than versions tags should be accepted.
		{"Version1.0.1", ""},
		{"Undefined", ""},
		// Accepted version tags.
		{"v1.0.1", "v1.0.1"},
		{"v2.2.2", "v2.2.2"},
		// Skipping *-alpha.00000000 tag.
		{"v1.0.1-alpha.00000000", ""},
		{"v2.2.2-alpha.00000000", ""},
		// Checking for An alpha/beta/rc tag.
		{"v1.0.1-alpha.1", "v1.0.1-alpha.1"},
		{"v1.0.1-beta.1", "v1.0.1-beta.1"},
		{"v1.0.1-rc.1", "v1.0.1-rc.1"},
		// Check is vX.Y.Z patch release >= .1 is first (ex: v20.1.1).
		{"v20.0.1", "v20.0.1"},
		{"v22.1.2", "v22.1.2"},
		// Checking for major releases.
		{"v1.1.0", "v1.1.0"},
		{"v2.2.0", "v2.2.0"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, matchVersion(tc.input), tc.expected)
		})
	}
}

func TestApiCall(t *testing.T) {
	skip.UnderStress(t, "Tests fail running in parallel due to the race condition in httptest binding the same port for"+
		" multiple server instances.")
	responseCodes := []int{201, 422, 500}
	for _, respCode := range responseCodes {
		t.Run(fmt.Sprintf("Resp code: %d", respCode), func(t *testing.T) {
			testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
				res.WriteHeader(respCode)
				_, err := res.Write([]byte("body"))
				if err != nil {
					return
				}
			}))
			defer testServer.Close()

			// Checking for any other error status "500"
			if respCode != 500 {
				err := apiCall(http.DefaultClient, testServer.URL, "test", "test")
				assert.NoError(t, err)
				return
			}
			// Checking for StatusUnprocessableEntity "422" and StatusCreated "201"
			err := apiCall(http.DefaultClient, testServer.URL, "test", "test")
			assert.Error(t, err)
		})
	}
}
