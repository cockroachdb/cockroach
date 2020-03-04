// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestUserName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		username   string
		normalized string
		err        string
		sqlstate   string
	}{
		{"Abc123", "abc123", "", ""},
		{"0123121132", "0123121132", "", ""},
		{"HeLlO", "hello", "", ""},
		{"Ομηρος", "ομηρος", "", ""},
		{"_HeLlO", "_hello", "", ""},
		{"a-BC-d", "a-bc-d", "", ""},
		{"A.Bcd", "a.bcd", "", ""},
		{"WWW.BIGSITE.NET", "www.bigsite.net", "", ""},
		{"", "", `username "" invalid`, pgcode.InvalidName},
		{"-ABC", "", `username "-abc" invalid`, pgcode.InvalidName},
		{".ABC", "", `username ".abc" invalid`, pgcode.InvalidName},
		{"*.wildcard", "", `username "\*.wildcard" invalid`, pgcode.InvalidName},
		{"foofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoof", "", `username "foofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoof" is too long`, pgcode.NameTooLong},
	}

	for _, tc := range testCases {
		normalized, err := sql.NormalizeAndValidateUsername(tc.username)
		if !testutils.IsError(err, tc.err) {
			t.Errorf("%q: expected %q, got %v", tc.username, tc.err, err)
			continue
		}
		if err != nil {
			if pgcode := pgerror.GetPGCode(err); pgcode != tc.sqlstate {
				t.Errorf("%q: expected SQLSTATE %s, got %s", tc.username, tc.sqlstate, pgcode)
				continue
			}
		}
		if normalized != tc.normalized {
			t.Errorf("%q: expected %q, got %q", tc.username, tc.normalized, normalized)
		}
	}
}
