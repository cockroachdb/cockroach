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

// toSQLStmt is a map of Kind -> SQL statement string for applying the
// option to the role.
var toSQLStmt = map[Option]string{
	CREATEROLE:   `UPSERT INTO system.role_options (username, option) VALUES ($1, 'CREATEROLE')`,
	NOCREATEROLE: `DELETE FROM system.role_options WHERE username = $1 AND option = 'CREATEROLE'`,
}

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

// GetSQLStmts returns a list of SQL stmts to apply each role option.
func (rol List) GetSQLStmts() (stmts []string, err error) {
	if len(rol) <= 0 {
		return stmts, nil
	}

	err = rol.CheckRoleOptionConflicts()
	if err != nil {
		return stmts, err
	}

	for _, ro := range rol {
		// Skip PASSWORD option.
		// Since PASSWORD still resides in system.users, we handle setting PASSWORD
		// outside of this set stmt.
		// TODO(richardjcai): migrate password to system.role_options
		if ro.Option == PASSWORD {
			continue
		}
		stmts = append(stmts, toSQLStmt[ro.Option])
	}

	return stmts, nil
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

			return hashedPassword, nil
		}
	}
	// Password option not found.
	return hashedPassword, errors.New("password not found in role options")
}
