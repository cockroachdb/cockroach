// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package privilege outlines the basic privilege system for cockroach.
package privilege

import (
	"bytes"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// Privilege represents a privilege parsed from an Access Privilege Inquiry
// Function's privilege string argument.
type Privilege struct {
	Kind Kind
	// Each privilege Kind has an optional "grant option" flag associated with
	// it. A role can only grant a privilege on an object to others if it is the
	// owner of the object or if it itself holds that privilege WITH GRANT OPTION
	// on the object. This replaced the CockroachDB-specific GRANT privilege.
	GrantOption bool
}

var _ redact.SafeFormatter = Privilege{}

// SafeFormat implements the redact.SafeFormatter interface.
func (k Privilege) SafeFormat(s redact.SafePrinter, _ rune) {
	s.Printf("[kind=%s grantOption=%t]", k.Kind, k.GrantOption)
}

// ObjectType represents objects that can have privileges.
type ObjectType string

var _ redact.SafeValue = ObjectType("")

// SafeValue makes ObjectType a redact.SafeValue.
func (k ObjectType) SafeValue() {}

const (
	// Any represents any object type.
	Any ObjectType = "any"
	// Database represents a database object.
	Database ObjectType = "database"
	// Schema represents a schema object.
	Schema ObjectType = "schema"
	// Table represents a table object.
	Table ObjectType = "table"
	// Type represents a type object.
	Type ObjectType = "type"
	// Sequence represents a sequence object.
	Sequence ObjectType = "sequence"
	// Routine represents a function or procedure object.
	Routine ObjectType = "routine"
	// Global represents global privileges.
	Global ObjectType = "global"
	// VirtualTable represents a virtual table object.
	VirtualTable ObjectType = "virtual_table"
	// ExternalConnection represents an external connection object.
	ExternalConnection ObjectType = "external_connection"
)

var isDescriptorBacked = map[ObjectType]bool{
	Database:           true,
	Schema:             true,
	Table:              true,
	Type:               true,
	Sequence:           true,
	Routine:            true,
	Global:             false,
	VirtualTable:       false,
	ExternalConnection: false,
}

// Predefined sets of privileges.
var (
	// AllPrivileges is populated during init.
	AllPrivileges         List
	ReadData              = List{SELECT}
	ReadWriteData         = List{SELECT, INSERT, DELETE, UPDATE}
	ReadWriteSequenceData = List{SELECT, UPDATE, USAGE}
	DBPrivileges          = List{ALL, BACKUP, CHANGEFEED, CONNECT, CREATE, DROP, RESTORE, TEMPORARY, ZONECONFIG}
	TablePrivileges       = List{ALL, BACKUP, CHANGEFEED, CREATE, DROP, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG, TRIGGER, REPLICATIONDEST, REPLICATIONSOURCE, MAINTAIN}
	SchemaPrivileges      = List{ALL, CREATE, CHANGEFEED, USAGE}
	TypePrivileges        = List{ALL, USAGE}
	RoutinePrivileges     = List{ALL, EXECUTE}
	// SequencePrivileges is appended with TablePrivileges as well. This is because
	// before v22.2 we treated Sequences the same as Tables. This is to avoid making
	// certain privileges unavailable after upgrade migration.
	// Note that "CREATE, CHANGEFEED, INSERT, DELETE, ZONECONFIG" are no-op privileges on sequences.
	SequencePrivileges = List{ALL, USAGE, SELECT, UPDATE, CREATE, CHANGEFEED, DROP, INSERT, DELETE, ZONECONFIG}
	GlobalPrivileges   = List{
		ALL, BACKUP, RESTORE, MODIFYCLUSTERSETTING, EXTERNALCONNECTION, VIEWACTIVITY, VIEWACTIVITYREDACTED,
		VIEWCLUSTERSETTING, CANCELQUERY, NOSQLLOGIN, VIEWCLUSTERMETADATA, VIEWDEBUG, EXTERNALIOIMPLICITACCESS, VIEWJOB,
		MODIFYSQLCLUSTERSETTING, REPLICATION, MANAGEVIRTUALCLUSTER, VIEWSYSTEMTABLE, CREATEROLE, CREATELOGIN, CREATEDB, CONTROLJOB,
		REPAIRCLUSTER, BYPASSRLS, REPLICATIONDEST, REPLICATIONSOURCE, INSPECT,
	}
	VirtualTablePrivileges       = List{ALL, SELECT}
	ExternalConnectionPrivileges = List{ALL, USAGE, DROP, UPDATE}
)

// Mask returns the bitmask for a given privilege.
func (k Kind) Mask() uint64 {
	return 1 << k
}

// IsSetIn returns true if this privilege kind is set in the supplied bitfield.
func (k Kind) IsSetIn(bits uint64) bool {
	return bits&k.Mask() != 0
}

// List is a list of privileges.
type List []Kind

var _ redact.SafeFormatter = List{}

// SafeFormat implements the redact.SafeFormatter interface.
func (pl List) SafeFormat(s interfaces.SafePrinter, _ rune) {
	if s.Flag('+') {
		s.SafeString("[")
	}
	for i, p := range pl {
		if i > 0 {
			s.SafeString(", ")
		}
		s.Print(p)
	}
	if s.Flag('+') {
		s.SafeString("]")
	}
}

// Len, Swap, and Less implement the Sort interface.
func (pl List) Len() int {
	return len(pl)
}

func (pl List) Swap(i, j int) {
	pl[i], pl[j] = pl[j], pl[i]
}

func (pl List) Less(i, j int) bool {
	return pl[i] < pl[j]
}

// names returns a list of privilege names in the same
// order as "pl".
func (pl List) names() []string {
	ret := make([]string, len(pl))
	for i, p := range pl {
		ret[i] = string(p.DisplayName())
	}
	return ret
}

// keys returns a list of privilege storage keys in the same
// order as "pl".
func (pl List) keys() []string {
	ret := make([]string, len(pl))
	for i, p := range pl {
		ret[i] = string(p.InternalKey())
	}
	return ret
}

// FormatNames prints out the list of display names in a buffer.
// This keeps the existing order and uses ", " as separator.
func (pl List) FormatNames(buf *bytes.Buffer) {
	for i, p := range pl {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(string(p.DisplayName()))
	}
}

// SortedDisplayNames returns a list of privilege display names
// in sorted order.
func (pl List) SortedDisplayNames() []string {
	names := pl.names()
	sort.Strings(names)
	return names
}

// SortedKeys returns a list of privilege internal keys
// in sorted order.
func (pl List) SortedKeys() []string {
	keys := pl.keys()
	sort.Strings(keys)
	return keys
}

// ToBitField returns the bitfield representation of
// a list of privileges.
func (pl List) ToBitField() uint64 {
	var ret uint64
	for _, p := range pl {
		ret |= p.Mask()
	}
	return ret
}

// Contains returns true iff the list contains the given privilege kind.
func (pl List) Contains(k Kind) bool {
	for _, p := range pl {
		if p == k {
			return true
		}
	}
	return false
}

// ListFromBitField takes a bitfield of privileges and a ObjectType
// returns a List. It is ordered in increasing value of privilege.Kind.
func ListFromBitField(m uint64, objectType ObjectType) (List, error) {
	ret := List{}

	privileges, err := GetValidPrivilegesForObject(objectType)
	if err != nil {
		return nil, err
	}

	for _, p := range privileges {
		if m&p.Mask() != 0 {
			ret = append(ret, p)
		}
	}
	return ret, nil
}

// PrivilegesFromBitFields takes a bitfield of privilege kinds, a bitfield of grant options, and an ObjectType
// returns a List. It is ordered in increasing value of privilege.Kind.
func PrivilegesFromBitFields(
	kindBits, grantOptionBits uint64, objectType ObjectType,
) ([]Privilege, error) {
	var ret []Privilege

	kinds, err := GetValidPrivilegesForObject(objectType)
	if err != nil {
		return nil, err
	}

	for _, kind := range kinds {
		if mask := kind.Mask(); kindBits&mask != 0 {
			ret = append(ret, Privilege{
				Kind:        kind,
				GrantOption: grantOptionBits&mask != 0,
			})
		}
	}
	return ret, nil
}

// Origin indicates the origin of the privileges being parsed in
// ListFromStrings.
type Origin bool

const (
	// OriginFromUserInput indicates that the privilege name came from user
	// input and should be validated to make sure it refers to a real privilege.
	OriginFromUserInput Origin = false

	// OriginFromSystemTable indicates that the privilege name came from a
	// system table and should be ignored if it does not refer to a real
	// privilege.
	OriginFromSystemTable Origin = true
)

// ListFromStrings takes a list of internal storage keys and attempts to build a
// list of Kind. Each string is converted to uppercase and is searched for
// either in ByInternalKey or in ByDisplayName maps, depending on the origin. If
// an entry is not found, it is either ignored or an error is raised (also
// depending on the origin).
func ListFromStrings(strs []string, origin Origin) (List, error) {
	ret := make(List, len(strs))
	for i, s := range strs {
		var k Kind
		switch origin {
		case OriginFromSystemTable:
			var ok bool
			k, ok = ByInternalKey[KindInternalKey(strings.ToUpper(s))]
			if !ok {
				// Ignore an unknown privilege name if it came from a system table. This
				// is so that it is possible to backport new privileges onto older release
				// branches, without causing mixed-version compatibility issues.
				continue
			}

		case OriginFromUserInput:
			var ok bool
			k, ok = ByDisplayName[KindDisplayName(strings.ToUpper(s))]
			if !ok {
				return nil, errors.Errorf("not a valid privilege: %q", s)
			}
		}
		ret[i] = k
	}
	return ret, nil
}

// ValidatePrivileges returns an error if any privilege in
// privileges cannot be granted on the given objectType.
func ValidatePrivileges(privileges List, objectType ObjectType) error {
	validPrivs, err := GetValidPrivilegesForObject(objectType)
	if err != nil {
		return err
	}
	for _, priv := range privileges {
		if validPrivs.ToBitField()&priv.Mask() == 0 {
			return pgerror.Newf(pgcode.InvalidGrantOperation,
				"invalid privilege type %s for %s", priv.DisplayName(), objectType)
		}
	}

	return nil
}

// GetValidPrivilegesForObject returns the list of valid privileges for the
// specified object type.
func GetValidPrivilegesForObject(objectType ObjectType) (List, error) {
	switch objectType {
	case Table:
		return TablePrivileges, nil
	case Schema:
		return SchemaPrivileges, nil
	case Database:
		return DBPrivileges, nil
	case Type:
		return TypePrivileges, nil
	case Sequence:
		return SequencePrivileges, nil
	case Any:
		return AllPrivileges, nil
	case Routine:
		return RoutinePrivileges, nil
	case Global:
		return GlobalPrivileges, nil
	case VirtualTable:
		return VirtualTablePrivileges, nil
	case ExternalConnection:
		return ExternalConnectionPrivileges, nil
	default:
		return nil, errors.AssertionFailedf("unknown object type %s", objectType)
	}
}

// ACLCharToPrivName maps PostgreSQL ACL abbreviation characters to privilege
// names as used in aclexplode output. This is the single source of truth for
// all ACL character ↔ privilege name mappings; the reverse map
// PrivNameToACLChar and the Kind-based map privToACL are derived from it in
// init().
var ACLCharToPrivName = map[byte]string{
	'r': "SELECT",
	'a': "INSERT",
	'w': "UPDATE",
	'd': "DELETE",
	'D': "TRUNCATE",
	'x': "REFERENCES",
	't': "TRIGGER",
	'X': "EXECUTE",
	'U': "USAGE",
	'C': "CREATE",
	'T': "TEMPORARY",
	'c': "CONNECT",
	'm': "MAINTAIN",
	's': "SET",
	'A': "ALTER SYSTEM",
}

// PrivNameToACLChar maps privilege names (uppercase) to PostgreSQL ACL
// abbreviation characters. Derived from ACLCharToPrivName in init().
var PrivNameToACLChar map[string]byte

// privToACL maps a CockroachDB privilege Kind to its PostgreSQL ACL
// abbreviation character string. Derived from ACLCharToPrivName in init().
var privToACL map[Kind]string

func init() {
	PrivNameToACLChar = make(map[string]byte, len(ACLCharToPrivName))
	for ch, name := range ACLCharToPrivName {
		PrivNameToACLChar[name] = ch
	}

	privToACL = make(map[Kind]string)
	for ch, name := range ACLCharToPrivName {
		if kind, ok := ByDisplayName[KindDisplayName(name)]; ok {
			privToACL[kind] = string(ch)
		}
	}
}

// ValidACLChars is the set of valid privilege characters in an aclitem string,
// matching PostgreSQL's ACL abbreviation characters. 'R' is accepted for
// backward compatibility (old RULE privilege) but is otherwise ignored.
const ValidACLChars = "arwdDxtXUCTcsAm"

// ValidateACLItemString validates that s has the PostgreSQL aclitem format:
//
//	grantee=privchars/grantor
//
// where grantee and grantor are role names (possibly double-quoted, or empty),
// and privchars is a sequence of privilege characters from ValidACLChars, each
// optionally followed by '*' indicating grant option. An empty grantee means
// PUBLIC. CockroachDB does not track grantors, so the grantor portion is
// typically empty but still must be present after the '/'.
func ValidateACLItemString(s string) error {
	i := 0
	// Parse grantee identifier (may be empty for PUBLIC).
	i = skipACLIdentifier(s, i)
	if i < 0 {
		return pgerror.Newf(pgcode.InvalidTextRepresentation,
			"unterminated quoted identifier: %q", s)
	}
	if i >= len(s) || s[i] != '=' {
		return pgerror.Newf(pgcode.InvalidTextRepresentation,
			"missing \"=\" sign: %q", s)
	}
	i++ // skip '='
	// Parse privilege characters and grant option markers.
	for i < len(s) && s[i] != '/' {
		c := s[i]
		if c == '*' {
			return pgerror.Newf(pgcode.InvalidTextRepresentation,
				"invalid mode character: \"*\" must follow a privilege character: %q", s)
		}
		if !isValidACLChar(c) {
			return pgerror.Newf(pgcode.InvalidTextRepresentation,
				"invalid mode character: %q", string(c))
		}
		i++
		// Skip optional '*' grant option marker.
		if i < len(s) && s[i] == '*' {
			i++
		}
	}
	if i < len(s) && s[i] == '/' {
		i++ // skip '/'
		// Parse grantor identifier (may be empty in CockroachDB).
		i = skipACLIdentifier(s, i)
		if i < 0 {
			return pgerror.Newf(pgcode.InvalidTextRepresentation,
				"unterminated quoted identifier: %q", s)
		}
	}
	// If '/' is missing entirely, treat as empty grantor (PG issues a warning
	// and defaults the grantor; CRDB doesn't track grantors so we just accept).
	if i != len(s) {
		return pgerror.Newf(pgcode.InvalidTextRepresentation,
			"extra characters after aclitem specification: %q", s)
	}
	return nil
}

// skipACLIdentifier advances past an optionally double-quoted identifier
// starting at position i in s. Returns the new position, or -1 if a
// quoted identifier is missing its closing double quote.
func skipACLIdentifier(s string, i int) int {
	if i >= len(s) {
		return i
	}
	if s[i] == '"' {
		// Quoted identifier: consume until closing unescaped quote.
		i++ // skip opening quote
		for i < len(s) {
			if s[i] == '"' {
				i++
				// Escaped double quote ("") continues the identifier.
				if i < len(s) && s[i] == '"' {
					i++
					continue
				}
				return i
			}
			i++
		}
		// Unterminated quote.
		return -1
	}
	// Unquoted identifier: alphanumeric, underscore, or high-bit chars.
	for i < len(s) && isACLIdentChar(s[i]) {
		i++
	}
	return i
}

// isACLIdentChar returns true for characters allowed in an unquoted ACL
// identifier, matching PostgreSQL's is_safe_acl_char for getid context.
func isACLIdentChar(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') || c == '_' || c >= 128
}

// QuoteACLIdentifier quotes a role name for use in an aclitem string,
// matching PostgreSQL's putid() behavior. The name is double-quoted if it
// contains any character that is not alphanumeric, underscore, or a high-bit
// byte. Internal double quotes are escaped by doubling them. An empty name
// is returned as-is (it represents PUBLIC in aclitem format).
func QuoteACLIdentifier(name string) string {
	if len(name) == 0 {
		return name
	}
	needsQuoting := false
	for i := 0; i < len(name); i++ {
		if !isACLIdentChar(name[i]) {
			needsQuoting = true
			break
		}
	}
	if !needsQuoting {
		return name
	}
	return lexbase.EscapeSQLIdent(name)
}

// ExtractACLIdentifier extracts and unquotes a (possibly double-quoted)
// identifier from s starting at position i, matching PostgreSQL's getid()
// behavior. Returns the unquoted name and the position after the identifier.
// If a quoted identifier has no closing quote, returns ("", -1).
func ExtractACLIdentifier(s string, i int) (string, int) {
	if i >= len(s) {
		return "", i
	}
	if s[i] == '"' {
		i++ // skip opening quote
		var buf strings.Builder
		for i < len(s) {
			if s[i] == '"' {
				i++
				// Doubled double quote is an escape; single one ends the identifier.
				if i < len(s) && s[i] == '"' {
					buf.WriteByte('"')
					i++
					continue
				}
				return buf.String(), i
			}
			buf.WriteByte(s[i])
			i++
		}
		return "", -1
	}
	start := i
	for i < len(s) && isACLIdentChar(s[i]) {
		i++
	}
	return s[start:i], i
}

// isValidACLChar returns true if c is a recognized privilege character.
func isValidACLChar(c byte) bool {
	return strings.IndexByte(ValidACLChars, c) >= 0
}

// ListToACL converts a list of privileges to a list of Postgres
// ACL items.
// See: https://www.postgresql.org/docs/13/ddl-priv.html#PRIVILEGE-ABBREVS-TABLE
//
//	for privileges and their ACL abbreviations.
func (pl List) ListToACL(grantOptions List, objectType ObjectType) (string, error) {
	privileges := pl
	// If ALL is present, explode ALL into the underlying privileges.
	if pl.Contains(ALL) {
		var err error
		privileges, err = GetValidPrivilegesForObject(objectType)
		if err != nil {
			return "", err
		}
		if grantOptions.Contains(ALL) {
			grantOptions, err = GetValidPrivilegesForObject(objectType)
			if err != nil {
				return "", err
			}
		}
	}
	// Sort privileges by Kind value so that ACL characters appear in a
	// deterministic order matching their bit positions.
	sorted := make(List, len(privileges))
	copy(sorted, privileges)
	sort.Sort(sorted)

	var chars []string
	for _, privilege := range sorted {
		aclChar, ok := privToACL[privilege]
		if !ok {
			// Skip privileges that have no PostgreSQL ACL character
			// equivalent (e.g. ALL after expansion, or CockroachDB-specific
			// privileges like BACKUP, CHANGEFEED, ZONECONFIG).
			continue
		}
		chars = append(chars, aclChar)
		if grantOptions.Contains(privilege) {
			chars = append(chars, "*")
		}
	}

	return strings.Join(chars, ""), nil

}

// IsDescriptorBacked returns whether o is a descriptor backed object.
// If o is not a descriptor backed object, then privileges are stored to
// system.privileges.
func (o ObjectType) IsDescriptorBacked() bool {
	return isDescriptorBacked[o]
}

// Object represents an object that can have privileges. The privileges
// can either live on the descriptor or in the system.privileges table.
type Object interface {
	// GetObjectType returns the privilege.ObjectType of the Object.
	GetObjectType() ObjectType
	// GetObjectTypeString returns a human-readable representation of the
	// privilege.ObjectType.
	// NOTE: It may not match the privilege.ObjectType directly because it may
	// be more specific for some object types. For example, for functions and
	// procedures it will return "function" and "procedure", respectively,
	// instead of the more generic term "routine".
	GetObjectTypeString() string
	// GetName returns the name of the object. For example, the name of a
	// table, schema or database.
	GetName() string
}
