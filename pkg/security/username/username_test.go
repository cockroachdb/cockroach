// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package username_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestUserName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		username   string
		normalized string
		err        string
	}{
		{"Abc123", "abc123", ""},
		{"0123121132", "0123121132", ""},
		{"HeLlO", "hello", ""},
		{"Ομηρος", "ομηρος", ""},
		{"_HeLlO", "_hello", ""},
		{"a-BC-d", "a-bc-d", ""},
		{"A.Bcd", "a.bcd", ""},
		{"WWW.BIGSITE.NET", "www.bigsite.net", ""},
		{"", "", `username is empty`},
		{"-ABC", "", `username is invalid`},
		{".ABC", "", `username is invalid`},
		{"*.wildcard", "", `username is invalid`},
		{"foofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoof", "", `username is too long`},
		{"M", "m", ""},
		{".", "", `username is invalid`},
	}

	for _, tc := range testCases {
		username, err := username.MakeSQLUsernameFromUserInput(tc.username, username.PurposeCreation)
		if !testutils.IsError(err, tc.err) {
			t.Errorf("%q: expected %q, got %v", tc.username, tc.err, err)
			continue
		}
		if err != nil {
			continue
		}
		if username.Normalized() != tc.normalized {
			t.Errorf("%q: expected %q, got %q", tc.username, tc.normalized, username)
		}
	}
}
