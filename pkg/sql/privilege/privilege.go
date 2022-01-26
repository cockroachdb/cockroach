// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package privilege

import (
	"bytes"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

//go:generate stringer -type=Kind

// Kind defines a privilege. This is output by the parser,
// and used to generate the privilege bitfields in the PrivilegeDescriptor.
type Kind uint32

// List of privileges. ALL is specifically encoded so that it will automatically
// pick up new privileges.
// Do not change values of privileges. These correspond to the position
// of the privilege in a bit field and are expected to stay constant.
const (
	ALL        Kind = 1
	CREATE     Kind = 2
	DROP       Kind = 3
	GRANT      Kind = 4
	SELECT     Kind = 5
	INSERT     Kind = 6
	DELETE     Kind = 7
	UPDATE     Kind = 8
	USAGE      Kind = 9
	ZONECONFIG Kind = 10
	CONNECT    Kind = 11
	RULE       Kind = 12
)

// Privilege represents a privilege parsed from an Access Privilege Inquiry
// Function's privilege string argument.
type Privilege struct {
	Kind Kind
	// Each privilege Kind has an optional "grant option" flag associated with
	// it. A role can only grant a privilege on an object to others if it is the
	// owner of the object or if it itself holds that privilege WITH GRANT OPTION
	// on the object. This replaces the CockroachDB-specific GRANT privilege.
	GrantOption bool
}

// ObjectType represents objects that can have privileges.
type ObjectType string

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
)

// Predefined sets of privileges.
var (
	AllPrivileges    = List{ALL, CONNECT, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, USAGE, ZONECONFIG}
	ReadData         = List{GRANT, SELECT}
	ReadWriteData    = List{GRANT, SELECT, INSERT, DELETE, UPDATE}
	DBPrivileges     = List{ALL, CONNECT, CREATE, DROP, GRANT, ZONECONFIG}
	TablePrivileges  = List{ALL, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG}
	SchemaPrivileges = List{ALL, GRANT, CREATE, USAGE}
	TypePrivileges   = List{ALL, GRANT, USAGE}
)

// Mask returns the bitmask for a given privilege.
func (k Kind) Mask() uint32 {
	return 1 << k
}

// IsSetIn returns true if this privilege kind is set in the supplied bitfield.
func (k Kind) IsSetIn(bits uint32) bool {
	return bits&k.Mask() != 0
}

// ByValue is just an array of privilege kinds sorted by value.
var ByValue = [...]Kind{
	ALL, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, USAGE, ZONECONFIG, CONNECT, RULE,
}

// ByName is a map of string -> kind value.
var ByName = map[string]Kind{
	"ALL":        ALL,
	"CONNECT":    CONNECT,
	"CREATE":     CREATE,
	"DROP":       DROP,
	"GRANT":      GRANT,
	"SELECT":     SELECT,
	"INSERT":     INSERT,
	"DELETE":     DELETE,
	"UPDATE":     UPDATE,
	"ZONECONFIG": ZONECONFIG,
	"USAGE":      USAGE,
	"RULE":       RULE,
}

// List is a list of privileges.
type List []Kind

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
		ret[i] = p.String()
	}
	return ret
}

// Format prints out the list in a buffer.
// This keeps the existing order and uses ", " as separator.
func (pl List) Format(buf *bytes.Buffer) {
	for i, p := range pl {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(p.String())
	}
}

// String implements the Stringer interface.
// This keeps the existing order and uses ", " as separator.
func (pl List) String() string {
	return strings.Join(pl.names(), ", ")
}

// SortedString is similar to String() but returns
// privileges sorted by name and uses "," as separator.
func (pl List) SortedString() string {
	names := pl.SortedNames()
	return strings.Join(names, ",")
}

// SortedNames returns a list of privilege names
// in sorted order.
func (pl List) SortedNames() []string {
	names := pl.names()
	sort.Strings(names)
	return names
}

// ToBitField returns the bitfield representation of
// a list of privileges.
func (pl List) ToBitField() uint32 {
	var ret uint32
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
func ListFromBitField(m uint32, objectType ObjectType) List {
	ret := List{}

	privileges := GetValidPrivilegesForObject(objectType)

	for _, p := range privileges {
		if m&p.Mask() != 0 {
			ret = append(ret, p)
		}
	}
	return ret
}

// PrivilegesFromBitFields takes a bitfield of privilege kinds, a bitfield of grant options, and an ObjectType
// returns a List. It is ordered in increasing value of privilege.Kind.
func PrivilegesFromBitFields(
	kindBits uint32, grantOptionBits uint32, objectType ObjectType,
) []Privilege {
	var ret []Privilege

	kinds := GetValidPrivilegesForObject(objectType)

	for _, kind := range kinds {
		if mask := kind.Mask(); kindBits&mask != 0 {
			ret = append(ret, Privilege{
				Kind:        kind,
				GrantOption: grantOptionBits&mask != 0,
			})
		}
	}
	return ret
}

// ListFromStrings takes a list of strings and attempts to build a list of Kind.
// We convert each string to uppercase and search for it in the ByName map.
// If an entry is not found in ByName, an error is returned.
func ListFromStrings(strs []string) (List, error) {
	ret := make(List, len(strs))
	for i, s := range strs {
		k, ok := ByName[strings.ToUpper(s)]
		if !ok {
			return nil, errors.Errorf("not a valid privilege: %q", s)
		}
		ret[i] = k
	}
	return ret, nil
}

// ValidatePrivileges returns an error if any privilege in
// privileges cannot be granted on the given objectType.
func ValidatePrivileges(privileges List, objectType ObjectType) error {
	validPrivs := GetValidPrivilegesForObject(objectType)
	for _, priv := range privileges {
		if validPrivs.ToBitField()&priv.Mask() == 0 {
			return pgerror.Newf(pgcode.InvalidGrantOperation,
				"invalid privilege type %s for %s", priv.String(), objectType)
		}
	}

	return nil
}

// GetValidPrivilegesForObject returns the list of valid privileges for the
// specified object type.
func GetValidPrivilegesForObject(objectType ObjectType) List {
	switch objectType {
	case Table:
		return TablePrivileges
	case Schema:
		return SchemaPrivileges
	case Database:
		return DBPrivileges
	case Type:
		return TypePrivileges
	case Any:
		return AllPrivileges
	default:
		panic(errors.AssertionFailedf("unknown object type %s", objectType))
	}
}

// privToACL is a map of privilege -> ACL character
var privToACL = map[Kind]string{
	CREATE:  "C",
	SELECT:  "r",
	INSERT:  "a",
	DELETE:  "d",
	UPDATE:  "w",
	USAGE:   "U",
	CONNECT: "c",
}

// orderedPrivs is the list of privileges sorted in alphanumeric order based on the ACL character -> CUacdrw
var orderedPrivs = List{CREATE, USAGE, INSERT, CONNECT, DELETE, SELECT, UPDATE}

// ListToACL converts a list of privileges to a list of Postgres
// ACL items.
// See: https://www.postgresql.org/docs/13/ddl-priv.html#PRIVILEGE-ABBREVS-TABLE
//     for privileges and their ACL abbreviations.
func (pl List) ListToACL(grantOptions List, objectType ObjectType) string {
	privileges := pl
	// If ALL is present, explode ALL into the underlying privileges.
	if pl.Contains(ALL) {
		privileges = GetValidPrivilegesForObject(objectType)
		if grantOptions.Contains(ALL) {
			grantOptions = GetValidPrivilegesForObject(objectType)
		}
	}
	chars := make([]string, len(privileges))
	for _, privilege := range orderedPrivs {
		if _, ok := privToACL[privilege]; !ok {
			panic(errors.AssertionFailedf("unknown privilege type %s", privilege.String()))
		}
		if privileges.Contains(privilege) {
			chars = append(chars, privToACL[privilege])
		}
		if grantOptions.Contains(privilege) {
			chars = append(chars, "*")
		}
	}

	return strings.Join(chars, "")

}
