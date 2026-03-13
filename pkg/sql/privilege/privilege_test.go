// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package privilege_test

import (
	"bytes"
	"fmt"
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

func TestValidateACLItemString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		input string
		err   string
	}{
		// Valid cases.
		{input: "user1=r/"},
		{input: "user1=r/grantor"},
		{input: "=rw/root"},
		{input: "user1=arwdDxtXUCTcsAm/"},
		{input: "user1=r*w/"},
		{input: "user1=/"},
		{input: "user1=r"},
		{input: `"user with spaces"=r/`},
		{input: `"user-1"=rw/`},
		{input: `"user""quote"=r/`},
		{input: `user1=r/"grantor-1"`},

		// Invalid cases.
		{input: "user1=z/", err: `invalid mode character: "z"`},
		{input: "user1rw/", err: `missing "=" sign`},
		{input: "user1=*r/", err: `"*" must follow a privilege character`},
		{input: "user1=r/grantor extra", err: "extra characters after aclitem specification"},
		{input: `"unterminated=r/`, err: "unterminated quoted identifier"},
		{input: `user1=r/"unterminated`, err: "unterminated quoted identifier"},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			err := privilege.ValidateACLItemString(tc.input)
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
			}
		})
	}
}

func TestQuoteACLIdentifier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		input    string
		expected string
	}{
		{input: "root", expected: "root"},
		{input: "user1", expected: "user1"},
		{input: "User1", expected: "User1"},
		{input: "user_name", expected: "user_name"},
		// Names with special characters get quoted.
		{input: "user-1", expected: `"user-1"`},
		{input: "user name", expected: `"user name"`},
		{input: "user=weird", expected: `"user=weird"`},
		{input: "user/slash", expected: `"user/slash"`},
		// Internal double quotes are doubled.
		{input: `user"quote`, expected: `"user""quote"`},
		// Empty name is returned as-is (represents PUBLIC).
		{input: "", expected: ""},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%q", tc.input), func(t *testing.T) {
			result := privilege.QuoteACLIdentifier(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractACLIdentifier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		input    string
		pos      int
		wantName string
		wantPos  int
	}{
		// Unquoted identifiers.
		{input: "root=r/", pos: 0, wantName: "root", wantPos: 4},
		{input: "user_1=r/", pos: 0, wantName: "user_1", wantPos: 6},
		{input: "=r/root", pos: 3, wantName: "root", wantPos: 7},
		// Empty identifier (e.g. PUBLIC grantee).
		{input: "=r/root", pos: 0, wantName: "", wantPos: 0},
		// Quoted identifiers.
		{input: `"user-1"=r/`, pos: 0, wantName: "user-1", wantPos: 8},
		{input: `"user name"=r/`, pos: 0, wantName: "user name", wantPos: 11},
		// Doubled double quote inside a quoted identifier.
		{input: `"user""quote"=r/`, pos: 0, wantName: `user"quote`, wantPos: 13},
		// Unterminated quote returns -1.
		{input: `"unterminated`, pos: 0, wantName: "", wantPos: -1},
		// Past end of string.
		{input: "abc", pos: 3, wantName: "", wantPos: 3},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s@%d", tc.input, tc.pos), func(t *testing.T) {
			name, pos := privilege.ExtractACLIdentifier(tc.input, tc.pos)
			require.Equal(t, tc.wantName, name)
			require.Equal(t, tc.wantPos, pos)
		})
	}
}

// TestQuoteACLIdentifierRoundTrip verifies that quoting a name and then
// extracting it produces the original name.
func TestQuoteACLIdentifierRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	names := []string{
		"root", "user1", "user-1", "user name", `user"quote`,
		"user=weird", "user/slash",
	}
	for _, name := range names {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			quoted := privilege.QuoteACLIdentifier(name)
			extracted, pos := privilege.ExtractACLIdentifier(quoted, 0)
			require.Equal(t, name, extracted)
			require.Equal(t, len(quoted), pos)
		})
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
