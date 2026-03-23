// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package privilege

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/redact"
)

// ACLItem represents a PostgreSQL-compatible aclitem: grantee=privchars/grantor.
// The PUBLIC role is represented by username.PublicRoleName() in the Grantee
// field, which formats as an empty grantee (e.g. "=rw/root").
//
// Privileges and GrantOptions must contain already-expanded privilege kinds
// (callers must not pass ALL). GrantOptions must be a subset of Privileges.
type ACLItem struct {
	Grantee      username.SQLUsername
	Grantor      username.SQLUsername
	Privileges   List
	GrantOptions List
}

// NewACLItem constructs an ACLItem. Privileges and grantOptions must be
// already-expanded (no ALL). GrantOptions should be a subset of Privileges.
func NewACLItem(grantee, grantor username.SQLUsername, privileges, grantOptions List) ACLItem {
	return ACLItem{
		Grantee:      grantee,
		Grantor:      grantor,
		Privileges:   privileges,
		GrantOptions: grantOptions,
	}
}

var _ redact.SafeFormatter = ACLItem{}

// SafeFormat implements redact.SafeFormatter. Role names are PII and printed
// without safe markers; privilege characters and structural characters
// (=, /, *) are safe.
func (a ACLItem) SafeFormat(w redact.SafePrinter, _ rune) {
	if !a.Grantee.IsPublicRole() {
		w.Print(QuoteACLIdentifier(a.Grantee.Normalized()))
	}
	w.SafeRune('=')
	a.writePrivChars(w)
	w.SafeRune('/')
	w.Print(QuoteACLIdentifier(a.Grantor.Normalized()))
}

// String implements fmt.Stringer via redact.StringWithoutMarkers.
func (a ACLItem) String() string {
	return redact.StringWithoutMarkers(a)
}

// ParseACLItem parses a PostgreSQL aclitem string of the form
// "grantee=privchars/grantor" and returns the structured ACLItem.
// An empty grantee is interpreted as PUBLIC (username.PublicRoleName()).
func ParseACLItem(s string) (ACLItem, error) {
	i := 0

	// Parse grantee.
	granteeName, i := ExtractACLIdentifier(s, i)
	if i < 0 {
		return ACLItem{}, pgerror.Newf(pgcode.InvalidTextRepresentation,
			"unterminated quoted identifier: %q", s)
	}
	if i >= len(s) || s[i] != '=' {
		return ACLItem{}, pgerror.Newf(pgcode.InvalidTextRepresentation,
			"missing \"=\" sign: %q", s)
	}
	i++ // skip '='

	// Parse privilege characters and grant option markers.
	var privs List
	var grantOpts List
	for i < len(s) && s[i] != '/' {
		c := s[i]
		if c == '*' {
			return ACLItem{}, pgerror.Newf(pgcode.InvalidTextRepresentation,
				"invalid mode character: \"*\" must follow a privilege character: %q", s)
		}
		if !isValidACLChar(c) {
			return ACLItem{}, pgerror.Newf(pgcode.InvalidTextRepresentation,
				"invalid mode character: %q", string(c))
		}
		privName := ACLCharToPrivName[c]
		kind, ok := ByDisplayName[KindDisplayName(privName)]
		if !ok {
			// Character is valid per PostgreSQL but has no CRDB Kind.
			// Skip it (matches ListToACL behavior of skipping unknown privs).
			i++
			if i < len(s) && s[i] == '*' {
				i++
			}
			continue
		}
		privs = append(privs, kind)
		i++
		if i < len(s) && s[i] == '*' {
			grantOpts = append(grantOpts, kind)
			i++
		}
	}

	// Parse grantor.
	var grantorName string
	if i < len(s) && s[i] == '/' {
		i++ // skip '/'
		grantorName, i = ExtractACLIdentifier(s, i)
		if i < 0 {
			return ACLItem{}, pgerror.Newf(pgcode.InvalidTextRepresentation,
				"unterminated quoted identifier: %q", s)
		}
	}

	if i != len(s) {
		return ACLItem{}, pgerror.Newf(pgcode.InvalidTextRepresentation,
			"extra characters after aclitem specification: %q", s)
	}

	grantee := username.PublicRoleName()
	if granteeName != "" {
		grantee = username.MakeSQLUsernameFromPreNormalizedString(granteeName)
	}
	grantor := username.MakeSQLUsernameFromPreNormalizedString(grantorName)

	return ACLItem{
		Grantee:      grantee,
		Grantor:      grantor,
		Privileges:   privs,
		GrantOptions: grantOpts,
	}, nil
}

// pgDefaultPublicPrivs lists the privileges granted to PUBLIC by default
// for each object type, matching CockroachDB's actual defaults (see
// NewBaseDatabasePrivilegeDescriptor, NewBaseFunctionPrivilegeDescriptor,
// etc.).
var pgDefaultPublicPrivs = map[ObjectType]List{
	Database: {CONNECT, TEMPORARY},
	Routine:  {EXECUTE},
	Type:     {USAGE},
}

// DefaultACLItems computes the expected default aclitem entries for the given
// object type and owner. Used by privilegeDescriptorToACLArray (to detect
// default privileges and return NULL) and by the acldefault builtin.
//
// The returned items follow PostgreSQL conventions:
//   - No '*' grant option markers for admin, root, or owner (implicit).
//   - Only privileges with a PG ACL character equivalent are included.
//   - PUBLIC entries come first.
func DefaultACLItems(objectType ObjectType, owner username.SQLUsername) ([]ACLItem, error) {
	ownerPrivs, err := GetValidPrivilegesForObject(objectType)
	if err != nil {
		return nil, err
	}
	// Filter to only privileges with a PG ACL character, excluding ALL.
	var filtered List
	for _, p := range ownerPrivs {
		if p == ALL {
			continue
		}
		if _, ok := privToACL[p]; ok {
			filtered = append(filtered, p)
		}
	}

	var items []ACLItem

	// PUBLIC entry (empty grantee sorts first).
	if publicPrivs, ok := pgDefaultPublicPrivs[objectType]; ok {
		items = append(items, NewACLItem(
			username.PublicRoleName(), owner, publicPrivs, nil,
		))
	}

	// Admin and root entries.
	items = append(items, NewACLItem(
		username.AdminRoleName(), owner, filtered, nil,
	))
	items = append(items, NewACLItem(
		username.RootUserName(), owner, filtered, nil,
	))

	// Owner entry (if different from admin and root).
	if !owner.IsAdminRole() && !owner.IsRootUser() {
		items = append(items, NewACLItem(owner, owner, filtered, nil))
	}

	return items, nil
}

// IsDefaultACL returns true if the given ACL items match the default
// privileges for the specified object type and owner. Comparison is
// order-independent and based on string representation.
func IsDefaultACL(
	items []ACLItem, objectType ObjectType, owner username.SQLUsername,
) (bool, error) {
	defaults, err := DefaultACLItems(objectType, owner)
	if err != nil {
		return false, err
	}
	if len(items) != len(defaults) {
		return false, nil
	}
	actualSet := make(map[string]struct{}, len(items))
	for _, item := range items {
		actualSet[item.String()] = struct{}{}
	}
	for _, d := range defaults {
		if _, ok := actualSet[d.String()]; !ok {
			return false, nil
		}
	}
	return true, nil
}

// writePrivChars writes the sorted privilege characters (with '*' grant
// option markers) to the SafePrinter. Privileges without a PostgreSQL ACL
// character equivalent are silently skipped.
func (a ACLItem) writePrivChars(w redact.SafePrinter) {
	sorted := make(List, len(a.Privileges))
	copy(sorted, a.Privileges)
	sort.Sort(sorted)

	for _, p := range sorted {
		ch, ok := privToACL[p]
		if !ok {
			continue
		}
		w.SafeString(redact.SafeString(ch))
		if a.GrantOptions.Contains(p) {
			w.SafeRune('*')
		}
	}
}
