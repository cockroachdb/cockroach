// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package privilegepb_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/privilegepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPrivilegeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		raw              uint32
		privileges       privilegepb.List
		stringer, sorted string
	}{
		{0, privilegepb.List{}, "", ""},
		// We avoid 0 as a privilege value even though we use 1 << privValue.
		{1, privilegepb.List{}, "", ""},
		{2, privilegepb.List{privilegepb.Privilege_ALL}, "ALL", "ALL"},
		{10, privilegepb.List{privilegepb.Privilege_ALL, privilegepb.Privilege_DROP_PRIVILEGE}, "ALL, DROP", "ALL,DROP"},
		{144, privilegepb.List{privilegepb.Privilege_GRANT, privilegepb.Privilege_DELETE}, "GRANT, DELETE", "DELETE,GRANT"},
		{2046,
			privilegepb.List{privilegepb.Privilege_ALL, privilegepb.Privilege_CREATE, privilegepb.Privilege_DROP_PRIVILEGE, privilegepb.Privilege_GRANT,
				privilegepb.Privilege_SELECT, privilegepb.Privilege_INSERT, privilegepb.Privilege_DELETE, privilegepb.Privilege_UPDATE, privilegepb.Privilege_USAGE, privilegepb.Privilege_ZONECONFIG},
			"ALL, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, USAGE, ZONECONFIG",
			"ALL,CREATE,DELETE,DROP,GRANT,INSERT,SELECT,UPDATE,USAGE,ZONECONFIG",
		},
	}

	for _, tc := range testCases {
		pl := privilegepb.ListFromBitField(tc.raw, privilegepb.Any)
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
