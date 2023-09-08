// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package privilege outlines the basic privilege system for cockroach.
package privilege

import (
	"bytes"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

//go:generate stringer -type=Kind -linecomment

// Kind defines a privilege. This is output by the parser,
// and used to generate the privilege bitfields in the PrivilegeDescriptor.
type Kind uint32

var _ redact.SafeValue = Kind(0)

// SafeValue makes Kind a redact.SafeValue.
func (k Kind) SafeValue() {}

// List of privileges. ALL is specifically encoded so that it will automatically
// pick up new privileges.
// Do not change values of privileges. These correspond to the position
// of the privilege in a bit field and are expected to stay constant.
const (
	ALL    Kind = 1
	CREATE Kind = 2
	DROP   Kind = 3
	// DEPRECATEDGRANT is a placeholder to make sure that 4 is not reused.
	// It was previously used for the GRANT privilege that has been replaced with the more granular Privilege.GrantOption.
	DEPRECATEDGRANT          Kind = 4 // GRANT
	SELECT                   Kind = 5
	INSERT                   Kind = 6
	DELETE                   Kind = 7
	UPDATE                   Kind = 8
	USAGE                    Kind = 9
	ZONECONFIG               Kind = 10
	CONNECT                  Kind = 11
	RULE                     Kind = 12
	MODIFYCLUSTERSETTING     Kind = 13
	EXTERNALCONNECTION       Kind = 14
	VIEWACTIVITY             Kind = 15
	VIEWACTIVITYREDACTED     Kind = 16
	VIEWCLUSTERSETTING       Kind = 17
	CANCELQUERY              Kind = 18
	NOSQLLOGIN               Kind = 19
	EXECUTE                  Kind = 20
	VIEWCLUSTERMETADATA      Kind = 21
	VIEWDEBUG                Kind = 22
	BACKUP                   Kind = 23
	RESTORE                  Kind = 24
	EXTERNALIOIMPLICITACCESS Kind = 25
	CHANGEFEED               Kind = 26
	VIEWJOB                  Kind = 27
	MODIFYSQLCLUSTERSETTING  Kind = 28
	REPLICATION              Kind = 29
	MANAGETENANT             Kind = 30
	VIEWSYSTEMTABLE          Kind = 31
	CREATEROLE               Kind = 32
	CREATELOGIN              Kind = 33
	CREATEDB                 Kind = 34
	CONTROLJOB               Kind = 35
	largestKind                   = CONTROLJOB
)

var isDeprecatedKind = map[Kind]bool{
	DEPRECATEDGRANT: true,
}

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
func (k Privilege) SafeFormat(s interfaces.SafePrinter, _ rune) {
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
	// Function represent a function object.
	Function ObjectType = "function"
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
	Function:           true,
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
	DBPrivileges          = List{ALL, BACKUP, CONNECT, CREATE, DROP, RESTORE, ZONECONFIG}
	TablePrivileges       = List{ALL, BACKUP, CHANGEFEED, CREATE, DROP, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG}
	SchemaPrivileges      = List{ALL, CREATE, USAGE}
	TypePrivileges        = List{ALL, USAGE}
	FunctionPrivileges    = List{ALL, EXECUTE}
	// SequencePrivileges is appended with TablePrivileges as well. This is because
	// before v22.2 we treated Sequences the same as Tables. This is to avoid making
	// certain privileges unavailable after upgrade migration.
	// Note that "CREATE, CHANGEFEED, INSERT, DELETE, ZONECONFIG" are no-op privileges on sequences.
	SequencePrivileges = List{ALL, USAGE, SELECT, UPDATE, CREATE, CHANGEFEED, DROP, INSERT, DELETE, ZONECONFIG}
	GlobalPrivileges   = List{
		ALL, BACKUP, RESTORE, MODIFYCLUSTERSETTING, EXTERNALCONNECTION, VIEWACTIVITY, VIEWACTIVITYREDACTED,
		VIEWCLUSTERSETTING, CANCELQUERY, NOSQLLOGIN, VIEWCLUSTERMETADATA, VIEWDEBUG, EXTERNALIOIMPLICITACCESS, VIEWJOB,
		MODIFYSQLCLUSTERSETTING, REPLICATION, MANAGETENANT, VIEWSYSTEMTABLE, CREATEROLE, CREATELOGIN, CREATEDB, CONTROLJOB,
	}
	VirtualTablePrivileges       = List{ALL, SELECT}
	ExternalConnectionPrivileges = List{ALL, USAGE, DROP}
)

// Mask returns the bitmask for a given privilege.
func (k Kind) Mask() uint64 {
	return 1 << k
}

// IsSetIn returns true if this privilege kind is set in the supplied bitfield.
func (k Kind) IsSetIn(bits uint64) bool {
	return bits&k.Mask() != 0
}

// ByName is a map of string -> kind value. It is populated by init.
var ByName map[string]Kind

// List is a list of privileges.
type List []Kind

var _ redact.SafeFormatter = List{}

// SafeFormat implements the redact.SafeFormatter interface.
func (pl List) SafeFormat(s interfaces.SafePrinter, _ rune) {
	s.SafeString("[")
	for i, p := range pl {
		if i > 0 {
			s.SafeString(",")
		}
		s.Print(p)
	}
	s.SafeString("]")
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

// ListFromStrings takes a list of strings and attempts to build a list of Kind.
// We convert each string to uppercase and search for it in the ByName map.
// If an entry is not found in ByName, it is either ignored or reports an error
// depending on the purpose.
func ListFromStrings(strs []string, origin Origin) (List, error) {
	ret := make(List, len(strs))
	for i, s := range strs {
		k, ok := ByName[strings.ToUpper(s)]
		if !ok {
			// Ignore an unknown privilege name if it came from a system table. This
			// is so that it is possible to backport new privileges onto older release
			// branches, without causing mixed-version compatibility issues.
			if origin == OriginFromSystemTable {
				continue
			} else if origin == OriginFromUserInput {
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
				"invalid privilege type %s for %s", priv.String(), objectType)
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
	case Function:
		return FunctionPrivileges, nil
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

// privToACL is a map of privilege -> ACL character
var privToACL = map[Kind]string{
	CREATE:  "C",
	SELECT:  "r",
	INSERT:  "a",
	DELETE:  "d",
	UPDATE:  "w",
	USAGE:   "U",
	CONNECT: "c",
	EXECUTE: "X",
}

// orderedPrivs is the list of privileges sorted in alphanumeric order based on the ACL character -> CUacdrwX
var orderedPrivs = List{CREATE, USAGE, INSERT, CONNECT, DELETE, SELECT, UPDATE, EXECUTE}

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
	chars := make([]string, len(privileges))
	for _, privilege := range orderedPrivs {
		if _, ok := privToACL[privilege]; !ok {
			return "", errors.AssertionFailedf("unknown privilege type %s", privilege.String())
		}
		if privileges.Contains(privilege) {
			chars = append(chars, privToACL[privilege])
		}
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
	// GetName returns the name of the object. For example, the name of a
	// table, schema or database.
	GetName() string
}

func init() {
	AllPrivileges = make([]Kind, 0, largestKind)
	ByName = make(map[string]Kind)

	for kind := ALL; kind <= largestKind; kind++ {
		if isDeprecatedKind[kind] {
			continue
		}
		AllPrivileges = append(AllPrivileges, kind)
		ByName[kind.String()] = kind
	}
}
