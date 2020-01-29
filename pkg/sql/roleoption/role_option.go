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
	"errors"
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
func (o Option) Mask() uint32 {
	return 1 << o
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

// ToSQLColumnName returns the SQL Column name of a role option.
func (o Option) ToSQLColumnName() string {
	return toSQLColumn[o]
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
	for i, option := range rol {
		if i > 0 {
			sb.WriteString(" , ")
		}
		sb.WriteString(fmt.Sprintf(
			`"%s" = %t`,
			option.Option.ToSQLColumnName(),
			toBool[option.Option]))
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

// GetHashedPassword returns the value of the password after hashing it.
// Returns error if no password option is found or if password is invalid.
func (rol List) GetHashedPassword() ([]byte, error) {
	var hashedPassword []byte
	for _, ro := range rol {
		if ro.Option == PASSWORD {
			if ro.IsNull {
				// Use empty byte array for hashedPassword.
				return hashedPassword, nil
			}
			password, err := ro.Value()
			if err != nil {
				return hashedPassword, err
			}
			if password == "" {
				return hashedPassword, security.ErrEmptyPassword
			}
			hashedPassword, err = security.HashPassword(password)
			if err != nil {
				return hashedPassword, err
			}
		}
	}
	// Password option not found.
	return hashedPassword, errors.New("password not found in role options")
}
