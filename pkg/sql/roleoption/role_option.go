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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

//go:generate stringer -type=Option -linecomment

// Option defines a role option. This is output by the parser
type Option uint32

// RoleOption represents an Option with a value.
type RoleOption struct {
	Option
	HasValue bool
	// Need to resolve value in Exec for the case of placeholders.
	Value func() (bool, string, error)
}

// KindList of role options.
const (
	_ Option = iota
	CREATEROLE
	NOCREATEROLE
	PASSWORD
	LOGIN
	NOLOGIN
	VALIDUNTIL // VALID UNTIL
	CONTROLJOB
	NOCONTROLJOB
	CONTROLCHANGEFEED
	NOCONTROLCHANGEFEED
	CREATEDB
	NOCREATEDB
	CREATELOGIN
	NOCREATELOGIN
	VIEWACTIVITY
	NOVIEWACTIVITY
	CANCELQUERY
	NOCANCELQUERY
	MODIFYCLUSTERSETTING
	NOMODIFYCLUSTERSETTING
	DEFAULTSETTINGS
	VIEWACTIVITYREDACTED
	NOVIEWACTIVITYREDACTED
	SQLLOGIN
	NOSQLLOGIN
	VIEWCLUSTERSETTING
	NOVIEWCLUSTERSETTING
)

// toSQLStmts is a map of Kind -> SQL statement string for applying the
// option to the role.
var toSQLStmts = map[Option]string{
	CREATEROLE:             `UPSERT INTO system.role_options (username, option) VALUES ($1, 'CREATEROLE')`,
	NOCREATEROLE:           `DELETE FROM system.role_options WHERE username = $1 AND option = 'CREATEROLE'`,
	LOGIN:                  `DELETE FROM system.role_options WHERE username = $1 AND option = 'NOLOGIN'`,
	NOLOGIN:                `UPSERT INTO system.role_options (username, option) VALUES ($1, 'NOLOGIN')`,
	VALIDUNTIL:             `UPSERT INTO system.role_options (username, option, value) VALUES ($1, 'VALID UNTIL', $2::timestamptz::string)`,
	CONTROLJOB:             `UPSERT INTO system.role_options (username, option) VALUES ($1, 'CONTROLJOB')`,
	NOCONTROLJOB:           `DELETE FROM system.role_options WHERE username = $1 AND option = 'CONTROLJOB'`,
	CONTROLCHANGEFEED:      `UPSERT INTO system.role_options (username, option) VALUES ($1, 'CONTROLCHANGEFEED')`,
	NOCONTROLCHANGEFEED:    `DELETE FROM system.role_options WHERE username = $1 AND option = 'CONTROLCHANGEFEED'`,
	CREATEDB:               `UPSERT INTO system.role_options (username, option) VALUES ($1, 'CREATEDB')`,
	NOCREATEDB:             `DELETE FROM system.role_options WHERE username = $1 AND option = 'CREATEDB'`,
	CREATELOGIN:            `UPSERT INTO system.role_options (username, option) VALUES ($1, 'CREATELOGIN')`,
	NOCREATELOGIN:          `DELETE FROM system.role_options WHERE username = $1 AND option = 'CREATELOGIN'`,
	VIEWACTIVITY:           `UPSERT INTO system.role_options (username, option) VALUES ($1, 'VIEWACTIVITY')`,
	NOVIEWACTIVITY:         `DELETE FROM system.role_options WHERE username = $1 AND option = 'VIEWACTIVITY'`,
	CANCELQUERY:            `UPSERT INTO system.role_options (username, option) VALUES ($1, 'CANCELQUERY')`,
	NOCANCELQUERY:          `DELETE FROM system.role_options WHERE username = $1 AND option = 'CANCELQUERY'`,
	MODIFYCLUSTERSETTING:   `UPSERT INTO system.role_options (username, option) VALUES ($1, 'MODIFYCLUSTERSETTING')`,
	NOMODIFYCLUSTERSETTING: `DELETE FROM system.role_options WHERE username = $1 AND option = 'MODIFYCLUSTERSETTING'`,
	SQLLOGIN:               `DELETE FROM system.role_options WHERE username = $1 AND option = 'NOSQLLOGIN'`,
	NOSQLLOGIN:             `UPSERT INTO system.role_options (username, option) VALUES ($1, 'NOSQLLOGIN')`,
	VIEWACTIVITYREDACTED:   `UPSERT INTO system.role_options (username, option) VALUES ($1, 'VIEWACTIVITYREDACTED')`,
	NOVIEWACTIVITYREDACTED: `DELETE FROM system.role_options WHERE username = $1 AND option = 'VIEWACTIVITYREDACTED'`,
	VIEWCLUSTERSETTING:     `UPSERT INTO system.role_options (username, option) VALUES ($1, 'VIEWCLUSTERSETTING')`,
	NOVIEWCLUSTERSETTING:   `DELETE FROM system.role_options WHERE username = $1 AND option = 'VIEWCLUSTERSETTING'`,
}

// Mask returns the bitmask for a given role option.
func (o Option) Mask() uint32 {
	return 1 << o
}

// ByName is a map of string -> kind value.
var ByName = map[string]Option{
	"CREATEROLE":             CREATEROLE,
	"NOCREATEROLE":           NOCREATEROLE,
	"PASSWORD":               PASSWORD,
	"LOGIN":                  LOGIN,
	"NOLOGIN":                NOLOGIN,
	"VALID UNTIL":            VALIDUNTIL,
	"CONTROLJOB":             CONTROLJOB,
	"NOCONTROLJOB":           NOCONTROLJOB,
	"CONTROLCHANGEFEED":      CONTROLCHANGEFEED,
	"NOCONTROLCHANGEFEED":    NOCONTROLCHANGEFEED,
	"CREATEDB":               CREATEDB,
	"NOCREATEDB":             NOCREATEDB,
	"CREATELOGIN":            CREATELOGIN,
	"NOCREATELOGIN":          NOCREATELOGIN,
	"VIEWACTIVITY":           VIEWACTIVITY,
	"NOVIEWACTIVITY":         NOVIEWACTIVITY,
	"CANCELQUERY":            CANCELQUERY,
	"NOCANCELQUERY":          NOCANCELQUERY,
	"MODIFYCLUSTERSETTING":   MODIFYCLUSTERSETTING,
	"NOMODIFYCLUSTERSETTING": NOMODIFYCLUSTERSETTING,
	"DEFAULTSETTINGS":        DEFAULTSETTINGS,
	"VIEWACTIVITYREDACTED":   VIEWACTIVITYREDACTED,
	"NOVIEWACTIVITYREDACTED": NOVIEWACTIVITYREDACTED,
	"SQLLOGIN":               SQLLOGIN,
	"NOSQLLOGIN":             NOSQLLOGIN,
	"VIEWCLUSTERSETTING":     VIEWCLUSTERSETTING,
	"NOVIEWCLUSTERSETTING":   NOVIEWCLUSTERSETTING,
}

// ToOption takes a string and returns the corresponding Option.
func ToOption(str string) (Option, error) {
	ret := ByName[strings.ToUpper(str)]
	if ret == 0 {
		return 0, pgerror.Newf(pgcode.Syntax, "unrecognized role option %s", str)
	}

	return ret, nil
}

// List is a list of role options.
type List []RoleOption

// GetSQLStmts returns a map of SQL stmts to apply each role option.
// Maps stmts to values (value of the role option).
func (rol List) GetSQLStmts(op string) (map[string]func() (bool, string, error), error) {
	if len(rol) <= 0 {
		return nil, nil
	}

	stmts := make(map[string]func() (bool, string, error), len(rol))

	err := rol.CheckRoleOptionConflicts()
	if err != nil {
		return stmts, err
	}

	for _, ro := range rol {
		sqltelemetry.IncIAMOptionCounter(
			op,
			strings.ToLower(ro.Option.String()),
		)
		// Skip PASSWORD and DEFAULTSETTINGS options.
		// Since PASSWORD still resides in system.users, we handle setting PASSWORD
		// outside of this set stmt.
		// DEFAULTSETTINGS is stored in system.database_role_settings.
		// TODO(richardjcai): migrate password to system.role_options
		if ro.Option == PASSWORD || ro.Option == DEFAULTSETTINGS {
			continue
		}

		stmt := toSQLStmts[ro.Option]
		if ro.HasValue {
			stmts[stmt] = ro.Value
		} else {
			stmts[stmt] = nil
		}
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

	if (roleOptionBits&CREATEROLE.Mask() != 0 &&
		roleOptionBits&NOCREATEROLE.Mask() != 0) ||
		(roleOptionBits&LOGIN.Mask() != 0 &&
			roleOptionBits&NOLOGIN.Mask() != 0) ||
		(roleOptionBits&CONTROLJOB.Mask() != 0 &&
			roleOptionBits&NOCONTROLJOB.Mask() != 0) ||
		(roleOptionBits&CONTROLCHANGEFEED.Mask() != 0 &&
			roleOptionBits&NOCONTROLCHANGEFEED.Mask() != 0) ||
		(roleOptionBits&CREATEDB.Mask() != 0 &&
			roleOptionBits&NOCREATEDB.Mask() != 0) ||
		(roleOptionBits&CREATELOGIN.Mask() != 0 &&
			roleOptionBits&NOCREATELOGIN.Mask() != 0) ||
		(roleOptionBits&VIEWACTIVITY.Mask() != 0 &&
			roleOptionBits&NOVIEWACTIVITY.Mask() != 0) ||
		(roleOptionBits&CANCELQUERY.Mask() != 0 &&
			roleOptionBits&NOCANCELQUERY.Mask() != 0) ||
		(roleOptionBits&MODIFYCLUSTERSETTING.Mask() != 0 &&
			roleOptionBits&NOMODIFYCLUSTERSETTING.Mask() != 0) ||
		(roleOptionBits&VIEWACTIVITYREDACTED.Mask() != 0 &&
			roleOptionBits&NOVIEWACTIVITYREDACTED.Mask() != 0) ||
		(roleOptionBits&SQLLOGIN.Mask() != 0 &&
			roleOptionBits&NOSQLLOGIN.Mask() != 0) ||
		(roleOptionBits&VIEWCLUSTERSETTING.Mask() != 0 &&
			roleOptionBits&NOVIEWCLUSTERSETTING.Mask() != 0) {
		return pgerror.Newf(pgcode.Syntax, "conflicting role options")
	}
	return nil
}

// GetPassword returns the value of the password or whether the
// password was set to NULL. Returns error if the string was invalid
// or if no password option is found.
func (rol List) GetPassword() (isNull bool, password string, err error) {
	for _, ro := range rol {
		if ro.Option == PASSWORD {
			return ro.Value()
		}
	}
	// Password option not found.
	return false, "", errors.New("password not found in role options")
}
