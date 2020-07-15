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
const (
	_ Kind = iota
	ALL
	// ALTER privilege is only used for types. Since CRDB does not have the
	// concept of ownership, ALTER privilege is required to ALTER the type.
	// Having ALTER + GRANT act as ownership for a type.
	ALTER
	CREATE
	DROP
	GRANT
	SELECT
	INSERT
	DELETE
	UPDATE
	USAGE
	ZONECONFIG
)

// ObjectType represents objects that can have privileges.
type ObjectType string

const (
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
	AllPrivileges           = List{ALL, ALTER, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, USAGE, ZONECONFIG}
	ReadData                = List{GRANT, SELECT}
	ReadWriteData           = List{GRANT, SELECT, INSERT, DELETE, UPDATE}
	DBSchemaTablePrivileges = List{ALL, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG}
	TypePrivileges          = List{ALL, ALTER, USAGE}
)

// Mask returns the bitmask for a given privilege.
func (k Kind) Mask() uint32 {
	return 1 << k
}

// ByValue is just an array of privilege kinds sorted by value.
var ByValue = [...]Kind{
	ALL, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, USAGE, ZONECONFIG,
}

// ByName is a map of string -> kind value.
var ByName = map[string]Kind{
	"ALL":        ALL,
	"ALTER":      ALTER,
	"CREATE":     CREATE,
	"DROP":       DROP,
	"GRANT":      GRANT,
	"SELECT":     SELECT,
	"INSERT":     INSERT,
	"DELETE":     DELETE,
	"UPDATE":     UPDATE,
	"ZONECONFIG": ZONECONFIG,
	"USAGE":      USAGE,
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
// order as 'pl'.
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

// ListFromBitField takes a bitfield of privileges and a ObjectType
// returns a List. It is ordered in increasing value of privilege.Kind.
func ListFromBitField(m uint32, objectType ObjectType) List {
	ret := List{}

	var privileges List

	switch objectType {
	case Database, Schema, Table:
		privileges = DBSchemaTablePrivileges
	case Type:
		privileges = TypePrivileges
	default:
		privileges = AllPrivileges
	}

	for _, p := range privileges {
		if m&p.Mask() != 0 {
			ret = append(ret, p)
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

// ValidateTypePrivileges returns an error if any privilege in privileges
// cannot be granted on a type object.
func ValidateTypePrivileges(privileges List) error {
	for _, priv := range privileges {
		if TypePrivileges.ToBitField()&priv.Mask() == 0 {
			return pgerror.Newf(pgcode.InvalidGrantOperation,
				"invalid privilege type %s for type", priv.String())
		}
	}

	return nil
}

// ValidateDBSchemaTablePrivileges returns an error if any privilege in
// privileges cannot be granted on a db/schema/table object.
// Currently db/schema/table can all be granted the same privileges.
func ValidateDBSchemaTablePrivileges(privileges List,
	objectType ObjectType,
) error {
	for _, priv := range privileges {
		// Check if priv is in DBSchemaTablePrivileges.
		if DBSchemaTablePrivileges.ToBitField()&priv.Mask() == 0 {
			return pgerror.Newf(pgcode.InvalidGrantOperation,
				"invalid privilege type %s for %s", priv.String(), objectType)
		}
	}

	return nil
}
