// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/pkg/errors"
)

//go:generate stringer -type=Option

// Option defines a role option. This is output by the parser
type Option uint32

type RoleOption struct {
	Option      Option
	Value       Expr
	ValueIsNull bool
}

// KindList of role options.
const (
	_ Option = iota
	CREATEROLE
	NOCREATEROLE
	LOGIN
	NOLOGIN
	PASSWORD
)

// Mask returns the bitmask for a given role option.
func (k Option) Mask() uint32 {
	return 1 << k
}

// ByName is a map of string -> kind value.
var ByName = map[string]Option{
	"CREATEROLE":   CREATEROLE,
	"NOCREATEROLE": NOCREATEROLE,
	"LOGIN":        LOGIN,
	"NOLOGIN":      NOLOGIN,
	"PASSWORD":     PASSWORD,
}

// MapToSQLColumn is a map of roleoption (Option) ->
// System.users SQL Column Name (string).
var MapToSQLColumn = map[Option]string{
	CREATEROLE:   "hasCreateRole",
	NOCREATEROLE: "hasCreateRole",
	LOGIN:        "login",
	NOLOGIN:      "login",
	PASSWORD:     "hashedPassword",
}

// MapToBool is a map of roleoption (Option) ->
// bool value in system.users table (bool).
var MapToBool = map[Option]bool{
	CREATEROLE:   true,
	NOCREATEROLE: false,
	LOGIN:        true,
	NOLOGIN:      false,
}

func ToOption(str string) (Option, error) {
	ret := ByName[strings.ToUpper(str)]
	if ret == 0 {
		return 0, pgerror.New(pgcode.Syntax, "option does not exist")
	}

	return ret, nil
}

// ToSQLColumnName returns the SQL Column Name corresponding to the Role option.
func (k Option) ToSQLColumnName() string {
	return MapToSQLColumn[k]
}

// ToSQLValue returns the value of option in it's SQL Column.
func (ro RoleOption) ToSQLValue() string {
	if ro.Option == PASSWORD {
		if ro.ValueIsNull {
			// Make sure password isn't used
			return "NULL"
		} else {
			// Need to hash the password here
			return fmt.Sprintf("'%s'", ro.Value)
		}
	}

	return strconv.FormatBool(MapToBool[ro.Option])
}

// KindList is a list of role option kinds.
type KindList []Option

// RoleOptionList is a list of role options.
type RoleOptionList []RoleOption

func (pl KindList) Len() int {
	return len(pl)
}

// names returns a list of role option names in the same
// order as 'pl'.
func (pl KindList) names() []string {
	ret := make([]string, len(pl))
	for i, p := range pl {
		ret[i] = p.String()
	}
	return ret
}

// Format prints out the list in a buffer.
// This keeps the existing order and uses " " as separator.
func (pl KindList) Format(buf *bytes.Buffer) {
	for i, p := range pl {
		if i > 0 {
			buf.WriteString(" ")
		}

		buf.WriteString(p.String())

		//if p == PASSWORD {
		//
		//}
	}
}

// String implements the Stringer interface.
// This keeps the existing order and uses " " as separator.
func (pl KindList) String() string {
	return strings.Join(pl.names(), " ")
}

// ToBitField returns the bitfield representation of
// a list of role options.
func (pl RoleOptionList) ToBitField() (uint32, error) {
	var ret uint32
	for _, p := range pl {
		if ret&p.Option.Mask() != 0 {
			return 0, pgerror.Newf(pgcode.Syntax, "redundant role option options")
		}
		ret |= p.Option.Mask()
	}
	return ret, nil
}

// CreateSetStmtFromRoleOptions returns a string of the form:
// "SET "optionA" = true, "optionB" = false".
func (pl RoleOptionList) CreateSetStmtFromRoleOptions() (string, error) {
	if len(pl) <= 0 {
		return "", pgerror.Newf(pgcode.Syntax, "no role options found")
	}
	setStmt := "SET "
	for i, roleOption := range pl {
		option := roleOption.Option
		format := ", \"%s\" = %s "
		if i == 0 {
			format = "\"%s\" = %s "
		}
		setStmt += fmt.Sprintf(format, option.ToSQLColumnName(), roleOption.ToSQLValue())
	}

	return setStmt, nil
}

// WIP LIST FROM STRINGS HAS TO BE LIST FROM ROLEOPTION
// OR ANOTHER WAY TO MAKE LIST EASILY FROM ROLEOPTION IN YACC
// ListFromStrings takes a list of strings and attempts to build a list of Option.
// We convert each string to uppercase and search for it in the ByName map.
// If an entry is not found in ByName, an error is returned.
func ListFromRoleOptions(strs []string) (KindList, error) {
	ret := make(KindList, len(strs))
	for i, s := range strs {
		k, ok := ByName[strings.ToUpper(s)]
		if !ok {
			return nil, errors.Errorf("not a valid role option: %q", s)
		}
		ret[i] = k
	}
	return ret, nil
}

func (pl RoleOptionList) Contains(p Option) bool {
	for _, ro := range pl {
		if ro.Option == p {
			return true
		}
	}

	return false
}

// CheckRoleOptionConflicts returns an error if two or more options conflict with each other.
func (pl RoleOptionList) CheckRoleOptionConflicts() error {
	roleOptionBits, err := pl.ToBitField()

	if err != nil {
		return err
	}

	if (roleOptionBits&CREATEROLE.Mask() != 0 &&
		roleOptionBits&NOCREATEROLE.Mask() != 0) ||
		(roleOptionBits&LOGIN.Mask() != 0 &&
			roleOptionBits&NOLOGIN.Mask() != 0) {
		return pgerror.Newf(pgcode.Syntax, "conflicting role option options")
	}
	return nil
}
