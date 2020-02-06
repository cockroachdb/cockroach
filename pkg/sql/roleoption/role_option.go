// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roleoption

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

//go:generate stringer -type=Option

// Option defines a role option. This is output by the parser
type Option uint32

type Value struct {
	Value  string
	IsNull bool
}

type RoleOption struct {
	Option
	HasValue bool
	Value
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

// toSQLColumn is a map of roleoption (Option) ->
// System.users SQL Column Name (string).
var toSQLColumn = map[Option]string{
	CREATEROLE:   "hasCreateRole",
	NOCREATEROLE: "hasCreateRole",
	LOGIN:        "login",
	NOLOGIN:      "login",
	PASSWORD:     "hashedPassword",
}

// toBool is a map of roleoption (Option) ->
// bool value in system.users table (bool).
var toBool = map[Option]bool{
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

// RoleOptionList is a list of role options.
type RoleOptionList []RoleOption

// CreateSetStmtFromRoleOptions returns a string of the form:
// "SET "optionA" = true, "optionB" = false".
func (pl RoleOptionList) CreateSetStmt() (string, error) {
	if len(pl) <= 0 {
		return "", pgerror.Newf(pgcode.Syntax, "no role options found")
	}
	setStmt := ""
	for i, roleOption := range pl {
		option := roleOption.Option
		format := ", \"%s\" = %s "
		if i == 0 {
			format = "\"%s\" = %s "
		}
		value, err := roleOption.toSQLValue()
		if err != nil {
			return "", err
		}
		setStmt += fmt.Sprintf(format, toSQLColumn[option], value)
	}

	return setStmt, nil
}

// ToBitField returns the bitfield representation of
// a list of role options.
func (pl RoleOptionList) ToBitField() (uint32, error) {
	var ret uint32
	for _, p := range pl {
		if ret&p.Option.Mask() != 0 {
			return 0, pgerror.Newf(pgcode.Syntax, "redundant role options")
		}
		ret |= p.Option.Mask()
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
		return pgerror.Newf(pgcode.Syntax, "conflicting role options")
	}
	return nil
}

// toSQLValue returns the value of option in it's SQL Column.
func (ro RoleOption) toSQLValue() (string, error) {
	// Password is a special case.
	if ro.Option == PASSWORD {
		if ro.Value.IsNull {
			return "NULL", nil
		}
		hashedPassword, err := security.HashPassword(ro.Value.Value)
		if err != nil {
			return "", err
		}
		return string(hashedPassword), nil
	}

	if ro.HasValue {
		if ro.Value.IsNull {
			return "NULL", nil
		} else {
			return ro.Value.Value, nil
		}
	}

	return strconv.FormatBool(toBool[ro.Option]), nil
}
