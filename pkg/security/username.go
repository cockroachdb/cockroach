// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"bytes"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// SQLUsername represents a username valid inside SQL.
//
// Note that SQL usernames are not just ASCII names: they can start
// with digits or contain only digits; they can contain certain
// punctuation, and they can contain non-ASCII unicode letters.
// For example, "123.-456" is a valid username.
// Therefore, care must be taken when assembling a string from a
// username for use in other contexts, e.g. to generate filenames:
// some escaping and/or quoting is likely necessary.
//
// Additionally, beware that usernames as manipulated client-side (in
// client drivers, in CLI commands) may not be the same as
// server-side; this is because usernames can be substituted during
// authentication. Additional care must be taken when deriving
// server-side strings in client code. It is always better to add an
// API server-side to assemble the string safely on the client's
// behalf.
//
// This datatype is more complex to a simple string so as to force
// usages to clarify when it is converted to/from strings.
// This complexity is necessary because in CockroachDB SQL, unlike in
// PostgreSQL, SQL usernames are case-folded and NFC-normalized when a
// user logs in, or when used as input to certain CLI commands or SQL
// statements. Then, "inside" CockroachDB, username strings are
// considered pre-normalized and can be used directly for comparisons,
// lookup etc.
//
// - The constructor MakeSQLUsernameFromUserInput() creates
//   a username from "external input".
//
// - The constructor MakeSQLUsernameFromPreNormalizedString()
//   creates a username when the caller can guarantee that
//   the input is already pre-normalized.
//
// For convenience, the SQLIdentifier() method also represents a
// username in the form suitable for input back by the SQL parser.
//
type SQLUsername struct {
	u string
}

// NodeUser is used by nodes for intra-cluster traffic.
const NodeUser = "node"

// NodeUserName is the SQLUsername for NodeUser.
func NodeUserName() SQLUsername { return SQLUsername{NodeUser} }

// IsNodeUser is true iff the username designates the node user.
func (s SQLUsername) IsNodeUser() bool { return s.u == NodeUser }

// RootUser is the default cluster administrator.
const RootUser = "root"

// RootUserName is the SQLUsername for RootUser.
func RootUserName() SQLUsername { return SQLUsername{RootUser} }

// IsRootUser is true iff the username designates the root user.
func (s SQLUsername) IsRootUser() bool { return s.u == RootUser }

// AdminRole is the default (and non-droppable) role with superuser privileges.
const AdminRole = "admin"

// AdminRoleName is the SQLUsername for AdminRole.
func AdminRoleName() SQLUsername { return SQLUsername{AdminRole} }

// IsAdminRole is true iff the username designates the admin role.
func (s SQLUsername) IsAdminRole() bool { return s.u == AdminRole }

// PublicRole is the special "public" pseudo-role.
// All users are implicit members of "public". The role cannot be created,
// dropped, assigned to another role, and is generally not listed.
// It can be granted privileges, implicitly granting them to all users (current and future).
const PublicRole = "public"

// PublicRoleName is the SQLUsername for PublicRole.
func PublicRoleName() SQLUsername { return SQLUsername{PublicRole} }

// IsPublicRole is true iff the username designates the public role.
func (s SQLUsername) IsPublicRole() bool { return s.u == PublicRole }

// Undefined is true iff the username is an empty string.
func (s SQLUsername) Undefined() bool { return len(s.u) == 0 }

// TestUser is used in tests.
const TestUser = "testuser"

// TestUserName is the SQLUsername for testuser.
func TestUserName() SQLUsername { return SQLUsername{TestUser} }

// MakeSQLUsernameFromUserInput normalizes a username string as
// entered in an ambiguous context into a SQL username (performs case
// folding and unicode normalization form C - NFC).
// If the purpose if UsernameCreation, the structure of the username
// is also checked. An error is returned if the validation fails.
// If the purpose is UsernameValidation, no error is returned.
func MakeSQLUsernameFromUserInput(u string, purpose UsernamePurpose) (res SQLUsername, err error) {
	// Perform case folding and NFC normalization.
	res.u = lexbase.NormalizeName(u)
	if purpose == UsernameCreation {
		err = res.ValidateForCreation()
	}
	return res, err
}

// UsernamePurpose indicates the purpose of the resulting
// SQLUsername in MakeSQLUsernameFromUserInput.
type UsernamePurpose bool

const (
	// UsernameCreation indicates that the SQLUsername is being
	// input for the purpose of creating a user account.
	// This causes MakeSQLUsernameFromUserInput to also enforce
	// structural restrictions on the username: which characters
	// are allowed and a maximum length.
	UsernameCreation UsernamePurpose = false

	// UsernameValidation indicates that the SQLUsername is
	// being input for the purpose of looking up an existing
	// user, or to compare with an existing username.
	// This skips the stuctural restrictions imposed
	// for the purpose UsernameCreation.
	UsernameValidation UsernamePurpose = true
)

const usernameHelp = "Usernames are case insensitive, must start with a letter, " +
	"digit or underscore, may contain letters, digits, dashes, periods, or underscores, and must not exceed 63 characters."

const maxUsernameLengthForCreation = 63

var validUsernameCreationRE = regexp.MustCompile(`^[\p{Ll}0-9_][---\p{Ll}0-9_.]*$`)

// ValidateForCreation checks that a username matches the structural
// restrictions for creation of a user account with that name.
func (s SQLUsername) ValidateForCreation() error {
	if len(s.u) == 0 {
		return ErrUsernameEmpty
	}
	if len(s.u) > maxUsernameLengthForCreation {
		return ErrUsernameTooLong
	}
	if !validUsernameCreationRE.MatchString(s.u) {
		return ErrUsernameInvalid
	}
	return nil
}

// ErrUsernameTooLong indicates that a username string was too
// long. It is returned by ValidateForCreation() or
// MakeSQLUserFromUserInput() with purpose UsernameCreation.
var ErrUsernameTooLong = errors.WithHint(errors.New("username is too long"), usernameHelp)

// ErrUsernameInvalid indicates that a username string contained
// invalid characters. It is returned by ValidateForCreation() or
// MakeSQLUserFromUserInput() with purpose UsernameCreation.
var ErrUsernameInvalid = errors.WithHint(errors.New("username is invalid"), usernameHelp)

// ErrUsernameEmpty indicates that an empty string was used as
// username. It is returned by ValidateForCreation() or
// MakeSQLUserFromUserInput() with purpose UsernameCreation.
var ErrUsernameEmpty = errors.WithHint(errors.New("username is empty"), usernameHelp)

// ErrUsernameNotNormalized indicates that a username
// was not pre-normalized during a conversion.
var ErrUsernameNotNormalized = errors.WithHint(errors.New("username is not normalized"),
	"The username should be converted to lowercase and unicode characters normalized to NFC.")

// MakeSQLUsernameFromPreNormalizedString takes a string containing a
// canonical username and converts it to a SQLUsername. The caller of
// this promises that the argument is pre-normalized. This conversion
// is cheap.
// Note: avoid using this function when processing strings
// in requests from external APIs.
// See also: MakeSQLUsernameFromPreNormalizedStringChecked().
func MakeSQLUsernameFromPreNormalizedString(u string) SQLUsername {
	return SQLUsername{u: u}
}

// MakeSQLUsernameFromPreNormalizedStringChecked takes a string,
// validates it is a prenormalized username, then converts it to
// a SQLUsername.
// See also: MakeSQLUsernameFromPreNormalizedString().
func MakeSQLUsernameFromPreNormalizedStringChecked(u string) (SQLUsername, error) {
	res := SQLUsername{u: lexbase.NormalizeName(u)}
	if res.u != u {
		return res, ErrUsernameNotNormalized
	}
	return res, nil
}

// Normalized returns the normalized username, suitable for equality
// comparison and lookups. The username is unquoted.
func (s SQLUsername) Normalized() string { return s.u }

// SQLIdentifier returns the normalized username in a form
// suitable for parsing as a SQL identifier.
// The identifier is quoted if it contains special characters
// or it is a reserved keyword.
func (s SQLUsername) SQLIdentifier() string {
	var buf bytes.Buffer
	lexbase.EncodeRestrictedSQLIdent(&buf, s.u, lexbase.EncNoFlags)
	return buf.String()
}

// Format implements the fmt.Formatter interface. It renders
// the username in normalized form.
func (s SQLUsername) Format(fs fmt.State, verb rune) {
	_, f := redact.MakeFormat(fs, verb)
	fmt.Fprintf(fs, f, s.u)
}

// LessThan is true iff the receiver sorts strictly before
// the given argument. This can be used e.g. in sort.Sort().
func (s SQLUsername) LessThan(u SQLUsername) bool {
	return s.u < u.u
}

// SQLUsernameProto is the wire representation of a SQLUsername.
type SQLUsernameProto string

// Decode turns the proto representation of a username back into its
// legitimate form.
func (s SQLUsernameProto) Decode() SQLUsername { return SQLUsername{u: string(s)} }

// EncodeProto turns a username into its proto representation.
func (s SQLUsername) EncodeProto() SQLUsernameProto { return SQLUsernameProto(s.u) }
