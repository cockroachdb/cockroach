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

	"github.com/pkg/errors"
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
	CREATE
	DROP
	GRANT
	SELECT
	INSERT
	DELETE
	UPDATE
	ZONECONFIG
)

// Predefined sets of privileges.
var (
	ReadData      = List{GRANT, SELECT}
	ReadWriteData = List{GRANT, SELECT, INSERT, DELETE, UPDATE}
)

// Mask returns the bitmask for a given privilege.
func (k Kind) Mask() uint32 {
	return 1 << k
}

// ByValue is just an array of privilege kinds sorted by value.
var ByValue = [...]Kind{
	ALL, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG,
}

// ByName is a map of string -> kind value.
var ByName = map[string]Kind{
	"ALL":        ALL,
	"CREATE":     CREATE,
	"DROP":       DROP,
	"GRANT":      GRANT,
	"SELECT":     SELECT,
	"INSERT":     INSERT,
	"DELETE":     DELETE,
	"UPDATE":     UPDATE,
	"ZONECONFIG": ZONECONFIG,
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

// ListFromBitField takes a bitfield of privileges and
// returns a list. It is ordered in increasing
// value of privilege.Kind.
func ListFromBitField(m uint32) List {
	ret := List{}
	for _, p := range ByValue {
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
