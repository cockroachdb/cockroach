// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package privilege_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPrivilegeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		raw              uint32
		privileges       privilege.List
		stringer, sorted string
	}{
		{0, privilege.List{}, "", ""},
		// We avoid 0 as a privilege value even though we use 1 << privValue.
		{1, privilege.List{}, "", ""},
		{2, privilege.List{privilege.ALL}, "ALL", "ALL"},
		{10, privilege.List{privilege.ALL, privilege.DROP}, "ALL, DROP", "ALL,DROP"},
		{144, privilege.List{privilege.GRANT, privilege.DELETE}, "GRANT, DELETE", "DELETE,GRANT"},
		{2046,
			privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP, privilege.GRANT,
				privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.USAGE, privilege.ZONECONFIG},
			"ALL, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, USAGE, ZONECONFIG",
			"ALL,CREATE,DELETE,DROP,GRANT,INSERT,SELECT,UPDATE,USAGE,ZONECONFIG",
		},
	}

	for _, tc := range testCases {
		pl := privilege.ListFromBitField(tc.raw, privilege.Any)
		if len(pl) != len(tc.privileges) {
			t.Fatalf("%+v: wrong privilege list from raw: %+v", tc, pl)
		}
		for i := 0; i < len(pl); i++ {
			if pl[i] != tc.privileges[i] {
				t.Fatalf("%+v: wrong privilege list from raw: %+v", tc, pl)
			}
		}
		if pl.String() != tc.stringer {
			t.Fatalf("%+v: wrong String() output: %q", tc, pl.String())
		}
		if pl.SortedString() != tc.sorted {
			t.Fatalf("%+v: wrong SortedString() output: %q", tc, pl.SortedString())
		}
	}
}
