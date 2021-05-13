package privilegepb

import (
	"bytes"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

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
	AllPrivileges    = List{Privilege_ALL, Privilege_CONNECT, Privilege_CREATE, Privilege_DROP_PRIVILEGE, Privilege_GRANT, Privilege_SELECT, Privilege_INSERT, Privilege_DELETE, Privilege_UPDATE, Privilege_USAGE, Privilege_ZONECONFIG}
	ReadData         = List{Privilege_GRANT, Privilege_SELECT}
	ReadWriteData    = List{Privilege_GRANT, Privilege_SELECT, Privilege_INSERT, Privilege_DELETE, Privilege_UPDATE}
	DBPrivileges     = List{Privilege_ALL, Privilege_CONNECT, Privilege_CREATE, Privilege_DROP_PRIVILEGE, Privilege_GRANT, Privilege_SELECT, Privilege_INSERT, Privilege_DELETE, Privilege_UPDATE, Privilege_ZONECONFIG}
	TablePrivileges  = List{Privilege_ALL, Privilege_CREATE, Privilege_DROP_PRIVILEGE, Privilege_GRANT, Privilege_SELECT, Privilege_INSERT, Privilege_DELETE, Privilege_UPDATE, Privilege_ZONECONFIG}
	SchemaPrivileges = List{Privilege_ALL, Privilege_GRANT, Privilege_CREATE, Privilege_USAGE}
	TypePrivileges   = List{Privilege_ALL, Privilege_GRANT, Privilege_USAGE}
)

// Mask returns the bitmask for a given privilege.
func (p Privilege) Mask() uint32 {
	return 1 << p
}

// ByValue is just an array of privilege kinds sorted by value.
var ByValue = [...]Privilege{
	Privilege_ALL, Privilege_CREATE, Privilege_DROP_PRIVILEGE, Privilege_GRANT, Privilege_SELECT, Privilege_INSERT, Privilege_DELETE, Privilege_UPDATE, Privilege_USAGE, Privilege_ZONECONFIG, Privilege_CONNECT,
}

// ByName is a map of string -> kind value.
var ByName = map[string]Privilege{
	"ALL":        Privilege_ALL,
	"CONNECT":    Privilege_CONNECT,
	"CREATE":     Privilege_CREATE,
	"DROP":       Privilege_DROP_PRIVILEGE,
	"GRANT":      Privilege_GRANT,
	"SELECT":     Privilege_SELECT,
	"INSERT":     Privilege_INSERT,
	"DELETE":     Privilege_DELETE,
	"UPDATE":     Privilege_UPDATE,
	"ZONECONFIG": Privilege_ZONECONFIG,
	"USAGE":      Privilege_USAGE,
}

// List is a list of privileges.
type List []Privilege

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
		ret[i] = p.StringOverride()
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
		buf.WriteString(p.StringOverride())
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
func (pl List) Contains(p Privilege) bool {
	for _, priv := range pl {
		if priv == p {
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
// Currently db/schema/table can all be granted the same privileges.
func ValidatePrivileges(privileges List, objectType ObjectType) error {
	validPrivs := GetValidPrivilegesForObject(objectType)
	for _, priv := range privileges {
		if validPrivs.ToBitField()&priv.Mask() == 0 {
			return pgerror.Newf(pgcode.InvalidGrantOperation,
				"invalid privilege type %s for %s", priv.StringOverride(), objectType)
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

var stringOverride = map[string]string{
	"DROP_PRIVILEGE": "DROP",
}

// StringOverride corrects the display name for the privilege.
// The String method auto-generated by proto may not correspond to the
// desired display name of the privilege.
func (p Privilege) StringOverride() string {
	s := p.String()
	if val, ok := stringOverride[s]; ok {
		return val
	}

	return s
}
