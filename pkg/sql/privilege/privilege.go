// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package privilege

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
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
	ALL, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE,
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

// Lists is a list of privilege lists
type Lists []List

// Contains returns whether the list of privilege lists contains exactly the
// privilege list represented by bitfield m. This intentionally treats ALL and
// {CREATE, ..., UPDATE} as distinct privilege lists.
func (pls Lists) Contains(m uint32) bool {
	for _, pl := range pls {
		if pl.ToBitField() == m {
			return true
		}
	}
	return false
}

// String stringifies each constituent list of privileges, groups each inside of
// {} braces, and joins them with " or ".
//
// Examples:
//   {{SELECT}}.String() -> "{SELECT}"
//   {{SELECT}, {SELECT, GRANT}}.String() -> "{SELECT} or {SELECT, GRANT}"
func (pls Lists) String() string {
	ret := make([]string, len(pls))
	for i, pl := range pls {
		ret[i] = fmt.Sprintf("{%s}", pl)
	}
	return strings.Join(ret, " or ")
}
