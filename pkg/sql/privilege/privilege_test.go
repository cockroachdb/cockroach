// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package privilege_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/leaktest"
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
		{2047,
			privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP, privilege.GRANT,
				privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE},
			"ALL, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE",
			"ALL,CREATE,DELETE,DROP,GRANT,INSERT,SELECT,UPDATE",
		},
	}

	for _, tc := range testCases {
		pl := privilege.ListFromBitField(tc.raw)
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
