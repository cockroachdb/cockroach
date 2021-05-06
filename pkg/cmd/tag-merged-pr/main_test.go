// Copyright 2021 The Cockroach Authors.
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
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/assert"
)

func TestGetPrNumber(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Test #1212 pass test", "1212"},
		{"FAIL", ""},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, getPrNumber(tc.input), tc.expected)
		})
	}
}

func TestReadToken(t *testing.T) {
	output, err := ioutil.TempFile("", "token_test_file")
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
	dat, _ := ioutil.ReadFile(output.Name())

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
		{`1111 Pull request #1111
						 2222 Merge request #2222
					   3333 Super pull request #3333
					   4444 Ultra pull request #4444`, []string(nil)},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, filterPullRequests(tc.input), tc.expected)
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
