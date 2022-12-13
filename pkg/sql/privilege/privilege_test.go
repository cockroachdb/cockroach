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
	"fmt"
	"github.com/stretchr/testify/require"
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
		{384, privilege.List{privilege.DELETE, privilege.UPDATE}, "DELETE, UPDATE", "DELETE,UPDATE"},
		{2046,
			privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP,
				privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.USAGE, privilege.ZONECONFIG},
			"ALL, CREATE, DROP, SELECT, INSERT, DELETE, UPDATE, USAGE, ZONECONFIG",
			"ALL,CREATE,DELETE,DROP,INSERT,SELECT,UPDATE,USAGE,ZONECONFIG",
		},
	}

	for _, tc := range testCases {
		raw := tc.raw
		t.Run(fmt.Sprintf("%d", raw), func(t *testing.T) {
			pl := privilege.ListFromBitField(raw, privilege.Any)
			require.Equal(t, pl, tc.privileges)
			require.Equal(t, pl.String(), tc.stringer)
			require.Equal(t, pl.SortedString(), tc.sorted)
		})
	}
}

func TestMinMaxKind(t *testing.T) {
	require.Equal(t, privilege.MinKind, privilege.ALL)
	require.Equal(t, privilege.MaxKind, privilege.CHANGEFEED)
}

func TestByName(t *testing.T) {
	require.Equal(t, privilege.ByName, map[string]privilege.Kind{
		"ALL":                      2,
		"BACKUP":                   8388608,
		"CANCELQUERY":              262144,
		"CHANGEFEED":               67108864,
		"CONNECT":                  2048,
		"CREATE":                   4,
		"DELETE":                   128,
		"DROP":                     8,
		"EXECUTE":                  1048576,
		"EXTERNALCONNECTION":       16384,
		"EXTERNALIOIMPLICITACCESS": 33554432,
		"INSERT":                   64,
		"MODIFYCLUSTERSETTING":     8192,
		"NOSQLLOGIN":               524288,
		"RESTORE":                  16777216,
		"RULE":                     4096,
		"SELECT":                   32,
		"UPDATE":                   256,
		"USAGE":                    512,
		"VIEWACTIVITY":             32768,
		"VIEWACTIVITYREDACTED":     65536,
		"VIEWCLUSTERMETADATA":      2097152,
		"VIEWCLUSTERSETTING":       131072,
		"VIEWDEBUG":                4194304,
		"ZONECONFIG":               1024,
	})
}
