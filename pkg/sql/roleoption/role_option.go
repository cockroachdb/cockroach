// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roleoption

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/security/distinguishedname"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

//go:generate stringer -type=Option -linecomment

// Option defines a role option. This is output by the parser
type Option uint32

// RoleOption represents an Option with a value.
type RoleOption struct {
	Option
	HasValue bool
	Value    func() (bool, string, error)
	Validate func(_ *cluster.Settings, _ username.SQLUsername, val string) error
}

// KindList of role options.
//
// NOTE: Before adding a role option (especially a non-postgres one), consider
// using a global privilege instead. Global privileges are inherited from role
// to user, but role options are not.
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
	// VIEWACTIVITY is responsible for controlling access to DB Console
	// endpoints that allow a user to view data in the UI without having
	// the Admin role. In addition, the VIEWACTIVITY role permits viewing
	// *unredacted* data in the `/nodes` and `/nodes_ui` endpoints which
	// display IP addresses and hostnames.
	VIEWACTIVITY
	NOVIEWACTIVITY
	CANCELQUERY
	NOCANCELQUERY
	MODIFYCLUSTERSETTING
	NOMODIFYCLUSTERSETTING
	VIEWACTIVITYREDACTED
	NOVIEWACTIVITYREDACTED
	REPLICATION
	NOREPLICATION
	SQLLOGIN
	NOSQLLOGIN
	VIEWCLUSTERSETTING
	NOVIEWCLUSTERSETTING
	SUBJECT
	BYPASSRLS
	NOBYPASSRLS
)

// ControlChangefeedDeprecationNoticeMsg is a user friendly notice which should be shown when CONTROLCHANGEFEED is used
//
// TODO(#94757): remove CONTROLCHANGEFEED entirely
const ControlChangefeedDeprecationNoticeMsg = "The role option CONTROLCHANGEFEED will be removed in a future release," +
	" please switch to using the CHANGEFEED privilege for target tables instead:" +
	" https://www.cockroachlabs.com/docs/stable/create-changefeed.html#required-privileges"

// toSQLStmts is a map of Kind -> SQL statement string for applying the
// option to the role.
var toSQLStmts = map[Option]string{
	CREATEROLE:             `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'CREATEROLE', $2) ON CONFLICT DO NOTHING`,
	NOCREATEROLE:           `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'CREATEROLE'`,
	LOGIN:                  `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'NOLOGIN'`,
	NOLOGIN:                `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'NOLOGIN', $2) ON CONFLICT DO NOTHING`,
	VALIDUNTIL:             `UPSERT INTO system.role_options (username, option, value, user_id) VALUES ($1, 'VALID UNTIL', $2::timestamptz::string, $3)`,
	CONTROLJOB:             `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'CONTROLJOB', $2) ON CONFLICT DO NOTHING`,
	NOCONTROLJOB:           `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'CONTROLJOB'`,
	CONTROLCHANGEFEED:      `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'CONTROLCHANGEFEED', $2) ON CONFLICT DO NOTHING`,
	NOCONTROLCHANGEFEED:    `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'CONTROLCHANGEFEED'`,
	CREATEDB:               `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'CREATEDB', $2) ON CONFLICT DO NOTHING`,
	NOCREATEDB:             `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'CREATEDB'`,
	CREATELOGIN:            `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'CREATELOGIN', $2) ON CONFLICT DO NOTHING`,
	NOCREATELOGIN:          `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'CREATELOGIN'`,
	VIEWACTIVITY:           `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'VIEWACTIVITY', $2) ON CONFLICT DO NOTHING`,
	NOVIEWACTIVITY:         `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'VIEWACTIVITY'`,
	CANCELQUERY:            `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'CANCELQUERY', $2) ON CONFLICT DO NOTHING`,
	NOCANCELQUERY:          `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'CANCELQUERY'`,
	MODIFYCLUSTERSETTING:   `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'MODIFYCLUSTERSETTING', $2) ON CONFLICT DO NOTHING`,
	NOMODIFYCLUSTERSETTING: `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'MODIFYCLUSTERSETTING'`,
	SQLLOGIN:               `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'NOSQLLOGIN'`,
	NOSQLLOGIN:             `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'NOSQLLOGIN', $2) ON CONFLICT DO NOTHING`,
	REPLICATION:            `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'REPLICATION', $2) ON CONFLICT DO NOTHING`,
	NOREPLICATION:          `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'REPLICATION'`,
	VIEWACTIVITYREDACTED:   `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'VIEWACTIVITYREDACTED', $2) ON CONFLICT DO NOTHING`,
	NOVIEWACTIVITYREDACTED: `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'VIEWACTIVITYREDACTED'`,
	VIEWCLUSTERSETTING:     `INSERT INTO system.role_options (username, option, user_id) VALUES ($1, 'VIEWCLUSTERSETTING', $2) ON CONFLICT DO NOTHING`,
	NOVIEWCLUSTERSETTING:   `DELETE FROM system.role_options WHERE username = $1 AND user_id = $2 AND option = 'VIEWCLUSTERSETTING'`,
	SUBJECT:                `UPSERT INTO system.role_options (username, option, value, user_id) VALUES ($1, 'SUBJECT', $2::string, $3)`,
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
	"VIEWACTIVITYREDACTED":   VIEWACTIVITYREDACTED,
	"NOVIEWACTIVITYREDACTED": NOVIEWACTIVITYREDACTED,
	"REPLICATION":            REPLICATION,
	"NOREPLICATION":          NOREPLICATION,
	"SQLLOGIN":               SQLLOGIN,
	"NOSQLLOGIN":             NOSQLLOGIN,
	"VIEWCLUSTERSETTING":     VIEWCLUSTERSETTING,
	"NOVIEWCLUSTERSETTING":   NOVIEWCLUSTERSETTING,
	"SUBJECT":                SUBJECT,
	"BYPASSRLS":              BYPASSRLS,
	"NOBYPASSRLS":            NOBYPASSRLS,
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

// MakeListFromKVOptions converts KVOptions to a List using
// typeAsString to convert exprs to strings.
func MakeListFromKVOptions(
	ctx context.Context,
	o tree.KVOptions,
	typeAsStringOrNull func(context.Context, tree.Expr) (func() (isNull bool, _ string, _ error), error),
) (List, error) {
	roleOptions := make(List, len(o))

	for i, ro := range o {
		// Role options are always stored as keywords in ro.Key by the
		// parser.
		option, err := ToOption(string(ro.Key))
		if err != nil {
			return nil, err
		}

		if ro.Value != nil {
			if ro.Value == tree.DNull {
				roleOptions[i] = RoleOption{
					Option: option, HasValue: true, Value: func() (bool, string, error) {
						return true, "", nil
					},
				}
			} else {
				strFn, err := typeAsStringOrNull(ctx, ro.Value)
				if err != nil {
					return nil, err
				}
				roleOptions[i] = RoleOption{
					Option: option, Value: strFn, HasValue: true,
				}
			}
		} else {
			roleOptions[i] = RoleOption{
				Option: option, HasValue: false,
			}
		}

		switch option {
		case SUBJECT:
			roleOptions[i].Validate = func(settings *cluster.Settings, u username.SQLUsername, s string) error {
				if err := base.CheckEnterpriseEnabled(settings, "SUBJECT role option"); err != nil {
					return err
				}
				if u.IsRootUser() {
					return errors.WithDetailf(
						pgerror.Newf(pgcode.InvalidParameterValue, "role %q cannot have a SUBJECT", u),
						"use the --%s CLI flag to configure root",
						cliflags.RootCertDistinguishedName.Name,
					)
				}
				if err := distinguishedname.ValidateDN(s); err != nil {
					return pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
				}
				return nil
			}
		}
	}

	return roleOptions, nil
}

// GetSQLStmts returns a map of SQL stmts to apply each role option.
// Maps stmts to values (value of the role option).
func (rol List) GetSQLStmts(onRoleOption func(Option)) (map[string]*RoleOption, error) {
	if len(rol) <= 0 {
		return nil, nil
	}

	stmts := make(map[string]*RoleOption, len(rol))

	err := rol.CheckRoleOptionConflicts()
	if err != nil {
		return stmts, err
	}

	for i := range rol {
		ro := &rol[i]
		if onRoleOption != nil {
			onRoleOption(ro.Option)
		}
		// Skip PASSWORD option.
		// Since PASSWORD still resides in system.users, we handle setting PASSWORD
		// outside of this set stmt.
		// TODO(richardjcai): migrate password to system.role_options
		if ro.Option == PASSWORD {
			continue
		}
		if ro.Option == BYPASSRLS || ro.Option == NOBYPASSRLS {
			return nil, unimplemented.NewWithIssuef(
				136910, "the BYPASSRLS and NOBYPASSRLS options for roles are not currently supported")
		}

		stmt := toSQLStmts[ro.Option]
		stmts[stmt] = ro
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
			roleOptionBits&NOVIEWCLUSTERSETTING.Mask() != 0) ||
		(roleOptionBits&REPLICATION.Mask() != 0 &&
			roleOptionBits&NOREPLICATION.Mask() != 0) {
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
