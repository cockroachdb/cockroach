// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package privilege_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestPrivilegeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		raw        uint64
		privileges privilege.List
	}{
		{0, privilege.List{}},
		// We avoid 0 as a privilege value even though we use 1 << privValue.
		{1, privilege.List{}},
		{2, privilege.List{privilege.ALL}},
		{10, privilege.List{privilege.ALL, privilege.DROP}},
		{384, privilege.List{privilege.DELETE, privilege.UPDATE}},
		{2046, privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP,
			privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.USAGE, privilege.ZONECONFIG},
		},
	}

	for _, tc := range testCases {
		pl, err := privilege.ListFromBitField(tc.raw, privilege.Any)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, tc.privileges, pl)
	}
}

func TestPrivilegeListFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		privileges         privilege.List
		redactable         redact.RedactableString
		formatNames        string
		sortedDisplayNames []string
		sortedKeys         []string
	}{
		{privilege.List{}, "", "", []string{}, []string{}},
		{privilege.List{privilege.ALL}, "ALL", "ALL", []string{"ALL"}, []string{"ALL"}},
		{privilege.List{privilege.UPDATE, privilege.DELETE}, "UPDATE, DELETE", "UPDATE, DELETE", []string{"DELETE", "UPDATE"}, []string{"DELETE", "UPDATE"}},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.redactable, redact.Sprint(tc.privileges))

		var buf bytes.Buffer
		tc.privileges.FormatNames(&buf)
		require.Equal(t, tc.formatNames, buf.String())

		require.Equal(t, tc.sortedDisplayNames, tc.privileges.SortedDisplayNames())
		require.Equal(t, tc.sortedKeys, tc.privileges.SortedKeys())
	}
}

// TestByDisplayNameHasAllPrivileges verifies that every privilege is present in
// ByDisplayName.
func TestByDisplayNameHasAllPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, kind := range privilege.AllPrivileges {
		resolvedKind, ok := privilege.ByDisplayName[kind.DisplayName()]
		require.True(t, ok)
		require.Equal(t, kind, resolvedKind)

		// It must also be possible to resolve the privilege using its
		// internal key as input.
		resolvedKind, ok = privilege.ByDisplayName[privilege.KindDisplayName(kind.InternalKey())]
		require.True(t, ok)
		require.Equal(t, kind, resolvedKind)
	}
}

// TestModifyPrivHasCorrespondingViewPriv checks that each MODIFY privilege has
// a corresponding VIEW privilege.
func TestModifyPrivHasCorrespondingViewPriv(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If you are adding a new MODIFY privilege that does not need a corresponding
	// VIEW privilege, you must add it to the exceptions below. Verify the
	// exception with the SQL Foundations or Product Security teams.
	modifyExceptions := map[privilege.Kind]struct{}{
		privilege.MODIFYSQLCLUSTERSETTING: {}, // covered by VIEWCLUSTERSETTING
	}

	for _, modifyPriv := range privilege.AllPrivileges {
		if !strings.HasPrefix(string(modifyPriv.DisplayName()), "MODIFY") {
			continue
		}
		if _, ok := modifyExceptions[modifyPriv]; ok {
			continue
		}
		suffix := strings.TrimPrefix(string(modifyPriv.DisplayName()), "MODIFY")
		foundCorrespondingViewPriv := false
		for _, relatedPriv := range privilege.AllPrivileges {
			if string(relatedPriv.DisplayName()) == ("VIEW" + suffix) {
				foundCorrespondingViewPriv = true
			}
		}
		require.True(t, foundCorrespondingViewPriv, "missing VIEW privilege for %s", modifyPriv.DisplayName())
	}
}
