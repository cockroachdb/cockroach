// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roleprivilege

import (
	"bytes"
	"strings"

	"github.com/pkg/errors"
)

//go:generate stringer -type=Kind

// Kind defines a role privilege. This is output by the parser
type Kind uint32

// List of role privileges. ALL is specifically encoded so that it will automatically
// pick up new role privileges.
const (
	_ Kind = iota
	CREATEROLE
	NOCREATEROLE
)

// Mask returns the bitmask for a given role privilege.
func (k Kind) Mask() uint32 {
	return 1 << k
}

// ByValue is just an array of role privilege kinds sorted by value.
var ByValue = [...]Kind{
	CREATEROLE,
	NOCREATEROLE,
}

// ByName is a map of string -> kind value.
var ByName = map[string]Kind{
	"CREATEROLE":   CREATEROLE,
	"NOCREATEROLE": NOCREATEROLE,
}

// List is a list of role privileges.
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

// names returns a list of role privilege names in the same
// order as 'pl'.
func (pl List) names() []string {
	ret := make([]string, len(pl))
	for i, p := range pl {
		ret[i] = p.String()
	}
	return ret
}

// Format prints out the list in a buffer.
// This keeps the existing order and uses " " as separator.
func (pl List) Format(buf *bytes.Buffer) {
	for i, p := range pl {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(p.String())
	}
}

// String implements the Stringer interface.
// This keeps the existing order and uses " " as separator.
func (pl List) String() string {
	return strings.Join(pl.names(), " ")
}

// ToBitField returns the bitfield representation of
// a list of role privileges.
func (pl List) ToBitField() (uint32, error) {
	var ret uint32
	for _, p := range pl {
		if ret&p.Mask() != 0 {
			return 0, errors.New("redundant permissions")
		}
		ret |= p.Mask()
	}
	return ret, nil
}

// ListFromStrings takes a list of strings and attempts to build a list of Kind.
// We convert each string to uppercase and search for it in the ByName map.
// If an entry is not found in ByName, an error is returned.
func ListFromStrings(strs []string) (List, error) {
	ret := make(List, len(strs))
	for i, s := range strs {
		k, ok := ByName[strings.ToUpper(s)]
		if !ok {
			return nil, errors.Errorf("not a valid role privilege: %q", s)
		}
		ret[i] = k
	}
	return ret, nil
}

// CheckRolePrivilegeConflicts returns an error if two or more options conflict with each other
func CheckRolePrivilegeConflicts(rolePrivilegeBits uint32) error {
	if (rolePrivilegeBits&CREATEROLE.Mask() != 0) &&
		(rolePrivilegeBits&NOCREATEROLE.Mask() != 0) {
		return errors.New("conflicting role privilege options")
	}
	return nil
}
