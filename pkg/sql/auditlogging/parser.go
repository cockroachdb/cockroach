// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auditlogging

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/rulebasedscanner"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// Parse parses the provided audit logging configuration.
func Parse(input string) (*AuditConfig, error) {
	tokens, err := rulebasedscanner.Tokenize(input)
	if err != nil {
		return nil, err
	}

	config := EmptyAuditConfig()
	config.Settings = make([]AuditSetting, len(tokens.Lines))
	// settingsRoleMap keeps track of the roles we've already written in the config
	settingsRoleMap := make(map[username.SQLUsername]interface{}, len(tokens.Lines))
	for i, line := range tokens.Lines {
		setting, err := parseAuditSetting(line)
		if err != nil {
			return nil, errors.Wrapf(
				pgerror.WithCandidateCode(err, pgcode.ConfigFile),
				"line %d", tokens.Linenos[i])
		}
		if _, exists := settingsRoleMap[setting.Role]; exists {
			return nil, errors.Newf("duplicate role listed: %v", setting.Role)
		}
		settingsRoleMap[setting.Role] = i
		config.Settings[i] = setting
		if setting.Role.Normalized() == allUserRole {
			config.allRoleAuditSettingIdx = i
		}
	}
	return config, nil
}

func parseAuditSetting(inputLine rulebasedscanner.Line) (setting AuditSetting, err error) {
	fieldIdx := 0
	expectedNumFields := 2
	setting.input = inputLine.Input
	line := inputLine.Tokens

	if len(line) > expectedNumFields {
		return setting, errors.WithHint(
			errors.New("too many fields specified"),
			"Expected only 2 fields (role, statement filter)")
	}

	// Read the user/role type.
	if len(line[fieldIdx]) > 1 {
		return setting, errors.WithHint(
			errors.New("multiple values specified for role"),
			"Specify exactly one role type per line.")
	}
	// Note we do not do any validation to ensure the input role exists as an actual role. This allows for
	// input roles to be arbitrary string values.
	setting.Role, err = username.MakeSQLUsernameFromUserInput(line[fieldIdx][0].Value, username.PurposeValidation)
	if err != nil {
		return setting, err
	}
	err = parseRole(setting.Role)
	if err != nil {
		return setting, err
	}
	// Parse statement filter
	fieldIdx++
	if fieldIdx >= len(line) {
		return setting, errors.New("end-of-line before statement filter specification")
	}
	if len(line[fieldIdx]) > 1 {
		return setting, errors.WithHint(
			errors.New("multiple values specified for statement filter"),
			"Specify exactly one statement filter per line.")
	}
	setting.IncludeStatements, err = parseStatementFilter(line[fieldIdx][0].Value)
	return setting, err
}

func parseRole(role username.SQLUsername) error {
	// Cannot use reserved role names.
	if role.IsPublicRole() || role.IsNoneRole() {
		return errors.Newf("cannot use reserved role name: '%s'", role.Normalized())
	}
	// Cannot use node user.
	if role.IsNodeUser() {
		return errors.Newf("cannot use reserved username: '%s'", role.Normalized())
	}
	// Cannot use 'pg_' prefix, reserved.
	if strings.HasPrefix(role.Normalized(), "pg_") {
		return errors.Newf("cannot use 'pg_' prefix in role name: '%s'", role.Normalized())
	}
	// Cannot use 'crdb_internal_' prefix, reserved.
	if strings.HasPrefix(role.Normalized(), "crdb_internal_") {
		return errors.Newf("cannot use 'crdb_internal_' prefix in role name: '%s'", role.Normalized())
	}
	return nil
}

// parseStatementFilter parses the statement filter field.
func parseStatementFilter(stmtFilter string) (bool, error) {
	val := strings.ToUpper(stmtFilter)
	switch val {
	case "ALL":
		return true, nil
	case "NONE":
		return false, nil
	default:
		return false, errors.WithHint(errors.Newf(
			`unknown statement filter: %q (valid filters include: "ALL", "NONE")`, stmtFilter,
		), "Statement filter value is normalized (i.e. All, all are valid inputs for ALL)")
	}
}
