// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"reflect"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// statementsForceControl tracks if a statement tag (or ALTER TABLE subcommand)
// is enabled or disabled forcefully by the user to use declarative schema changer.
type statementsForceControl struct {
	// statements maps statement tags (e.g., "ALTER TABLE", "CREATE SCHEMA") to
	// their force control state (true=enabled, false=disabled).
	statements map[string]bool
	// subcommands maps ALTER TABLE subcommand tags (e.g., "ALTER TABLE ADD COLUMN")
	// to their force control state. Subcommand-level controls take precedence over
	// statement-level controls.
	subcommands map[string]bool
}

// supportedAlterTableSubcommandTags maps ALTER TABLE subcommand tags
// (e.g., "ALTER TABLE ADD COLUMN") to empty struct for validation.
var supportedAlterTableSubcommandTags = map[string]struct{}{}

// forceDeclarativeStatements outlines statements which are forcefully enabled
// and/or disabled with declarative schema changer, separated by comma.
// Forcefully enabled statements are prefixed with "+";
// Forcefully disabled statements are prefixed with "!";
//
// Statement-level control:
//
//	SET CLUSTER SETTING sql.schema.force_declarative_statements = '+ALTER TABLE,!CREATE SEQUENCE';
//
// ALTER TABLE subcommand-level control (subcommand names use uppercase with spaces):
//
//	SET CLUSTER SETTING sql.schema.force_declarative_statements = '!ALTER TABLE ADD COLUMN';
//	SET CLUSTER SETTING sql.schema.force_declarative_statements = '!ALTER TABLE, +ALTER TABLE ADD COLUMN';
//
// Subcommand-level controls take precedence over statement-level controls.
//
// Note: We can only control statements implemented in declarative schema changer.
var forceDeclarativeStatements = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"sql.schema.force_declarative_statements",
	"forcefully enable or disable declarative schema changer for specific statements",
	"",
	settings.WithValidateString(func(values *settings.Values, s string) error {
		if s == "" {
			return nil
		}
		// First split the string into individual tags.
		tags := strings.Split(s, ",")
		for _, tag := range tags {
			tag = strings.ToUpper(strings.TrimSpace(tag))
			if len(tag) > 0 && (tag[0] == '+' || tag[0] == '!') {
				tag = tag[1:]
			} else {
				return errors.Errorf("tag is not properly formatted, must start with '+' or '!' (%s)", tag)
			}
			// Check if this is an ALTER TABLE subcommand tag.
			sortedKeys := func(m map[string]struct{}) []string {
				keys := make([]string, 0, len(m))
				for k := range m {
					keys = append(keys, k)
				}
				slices.Sort(keys)
				return keys
			}
			if strings.HasPrefix(tag, "ALTER TABLE ") {
				if _, ok := supportedAlterTableSubcommandTags[tag]; !ok {
					return errors.WithDetail(
						errors.Errorf("ALTER TABLE subcommand tag %q is not controlled by the declarative schema changer", tag),
						"supported tags: "+strings.Join(sortedKeys(supportedAlterTableSubcommandTags), ", "))
				}
			} else if _, ok := supportedStatementTags[tag]; !ok {
				return errors.WithDetail(
					errors.Errorf("statement tag %q is not controlled by the declarative schema changer", tag),
					"supported tags: "+strings.Join(sortedKeys(supportedStatementTags), ", "))
			}
		}
		return nil
	}))

// CheckControl checks if a statement is forced to be enabled or disabled. If
// `n` is forcefully disabled, then a "NotImplemented" error will be panicked.
// Otherwise, return whether `n` is forcefully enabled.
func (c statementsForceControl) CheckControl(n tree.Statement) (forceEnabled bool) {
	if c.statements == nil {
		return false
	}
	enabledOrDisabled, found := c.statements[n.StatementTag()]
	if !found {
		return false
	}
	if !enabledOrDisabled {
		panic(scerrors.NotImplementedErrorf(n,
			"statement has been disabled via cluster setting"))
	}
	return enabledOrDisabled
}

// CheckAlterTableCmdControl checks if an ALTER TABLE subcommand is forced to
// be enabled or disabled. Subcommand-level controls take precedence over
// statement-level controls.
//
// Returns:
//   - (true, true): subcommand is force-enabled
//   - (false, true): subcommand is force-disabled
//   - (_, false): no subcommand-level control found
func (c statementsForceControl) CheckAlterTableCmdControl(
	cmd tree.AlterTableCmd,
) (enabled, found bool) {
	if c.subcommands == nil {
		return false, false
	}
	tag := alterTableCmdToTag(cmd)
	enabledOrDisabled, ok := c.subcommands[tag]
	if !ok {
		return false, false
	}
	return enabledOrDisabled, true
}

// alterTableCmdToTag converts an ALTER TABLE subcommand to its tag format.
// E.g., an AlterTableAddColumn command becomes "ALTER TABLE ADD COLUMN".
// Returns empty string if the command type is not in the alterTableSubcommandNames map.
func alterTableCmdToTag(cmd tree.AlterTableCmd) string {
	subcommandName, ok := alterTableSubcommandNames[reflect.TypeOf(cmd)]
	if !ok {
		return ""
	}
	return "ALTER TABLE " + subcommandName
}

// getStatementsForceControl returns a control structure that tracks statements
// and ALTER TABLE subcommands that are explicitly enabled or disabled by
// administrators for the declarative schema changer.
func getStatementsForceControl(sv *settings.Values) statementsForceControl {
	settingValue := forceDeclarativeStatements.Get(sv)
	var control statementsForceControl
	for _, tag := range strings.Split(settingValue, ",") {
		tag = strings.ToUpper(strings.TrimSpace(tag))
		if len(tag) == 0 {
			continue
		}
		enabledOrDisabled := true
		tagStart := tag[0]
		tag = tag[1:]
		// If the tag starts with a ! its disabled.
		if tagStart == '!' {
			enabledOrDisabled = false
		}
		// Check if this is an ALTER TABLE subcommand tag.
		if strings.HasPrefix(tag, "ALTER TABLE ") {
			if control.subcommands == nil {
				control.subcommands = make(map[string]bool)
			}
			control.subcommands[tag] = enabledOrDisabled
		} else {
			if control.statements == nil {
				control.statements = make(map[string]bool)
			}
			control.statements[tag] = enabledOrDisabled
		}
	}
	return control
}
