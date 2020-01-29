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

// RoleOption represents an Option with a value.
type RoleOption struct {
	Option
	HasValue bool
	// Need to resolve value in Exec for the case of placeholders.
	Value  func() (string, error)
	IsNull bool
}

// KindList of role options.
const (
	_ Option = iota
	CREATEROLE
	NOCREATEROLE
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
	"PASSWORD":     PASSWORD,
}

// toSQLColumn is a map of roleoption (Option) ->
// System.users SQL Column Name (string).
var toSQLColumn = map[Option]string{
	CREATEROLE:   "hasCreateRole",
	NOCREATEROLE: "hasCreateRole",
	PASSWORD:     "hashedPassword",
}

// toBool is a map of roleoption (Option) ->
// bool value in system.users table (bool).
var toBool = map[Option]bool{
	CREATEROLE:   true,
	NOCREATEROLE: false,
}

// ToOption takes a string and returns the corresponding Option.
func ToOption(str string) (Option, error) {
	ret := ByName[strings.ToUpper(str)]
	if ret == 0 {
		return 0, pgerror.New(pgcode.Syntax, "option does not exist")
	}

	return ret, nil
}

// List is a list of role options.
type List []RoleOption

// CreateSetStmt returns a string of the form:
// "SET "optionA" = true, "optionB" = false".
func (rol List) CreateSetStmt() (string, error) {
	if len(rol) <= 0 {
		return "", pgerror.Newf(pgcode.Syntax, "no role options found")
	}
	var sb strings.Builder
	for i, roleOption := range rol {
		option := roleOption.Option
		format := ", \"%s\" = %s "
		if i == 0 {
			format = "\"%s\" = %s "
		}
		value, err := roleOption.toSQLValue()
		if err != nil {
			return "", err
		}
		sb.WriteString(fmt.Sprintf(format, toSQLColumn[option], value))
	}

	return sb.String(), nil
}

// ToBitField returns the bitfield representation of
// a list of role options.
func (rol List) ToBitField() (uint32, error) {
	var ret uint32
	for _, p := range rol {
		if ret&p.Option.Mask() != 0 {
			return 0, pgerror.Newf(pgcode.Syntax, "redundant role options")
		}
		ret |= p.Option.Mask()
	}
	return ret, nil
}

// Contains returns true if List contains option, false otherwise.
func (rol List) Contains(p Option) bool {
	for _, ro := range rol {
		if ro.Option == p {
			return true
		}
	}

	return false
}

// CheckRoleOptionConflicts returns an error if two or more options conflict with each other.
func (rol List) CheckRoleOptionConflicts() error {
	roleOptionBits, err := rol.ToBitField()

	if err != nil {
		return err
	}

	if roleOptionBits&CREATEROLE.Mask() != 0 &&
		roleOptionBits&NOCREATEROLE.Mask() != 0 {
		return pgerror.Newf(pgcode.Syntax, "conflicting role options")
	}
	return nil
}

func (ro RoleOption) toSQLValue() (string, error) {
	// Password is a special case.
	if ro.Option == PASSWORD {
		if ro.IsNull {
			// Use empty string for hashedPassword.
			return "''", nil
		}
		password, err := ro.Value()
		if err != nil {
			return "", err
		}
		if password == "" {
			return "", security.ErrEmptyPassword
		}
		hashedPassword, err := security.HashPassword(password)
		return fmt.Sprintf("'%s'", hashedPassword), err
	}

	if ro.HasValue {
		if ro.IsNull {
			return "NULL", nil
		}
		value, err := ro.Value()
		if err != nil {
			return "", err
		}
		return value, nil
	}

	return strconv.FormatBool(toBool[ro.Option]), nil
}
