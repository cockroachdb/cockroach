// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// statementsForceControl track if a statement tag is enabled or disabled
// forcefully by the user to use declarative schema changer.
type statementsForceControl map[string]bool

// forceDeclarativeStatements outlines statements which are forcefully enabled
// and/or disabled with declarative schema changer, separated by comma.
// Forcefully enabled statements are prefixed with "+";
// Forcefully disabled statements are prefixed with "!";
// E.g. `SET CLUSTER SETTING sql.schema.force_declarative_statements = "+ALTER TABLE,!CREATE SEQUENCE";`
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
			if _, ok := supportedStatementTags[tag]; !ok {
				return errors.Errorf("statement tag %q is not controlled by the declarative schema changer", tag)
			}
		}
		return nil
	}))

// CheckControl checks if a statement is forced to be enabled or disabled. If
// `n` is forcefully disabled, then a "NotImplemented" error will be panicked.
// Otherwise, return whether `n` is forcefully enabled.
func (c statementsForceControl) CheckControl(n tree.Statement) (forceEnabled bool) {
	// This map is only created *if* any force flags are set.
	if c == nil {
		return false
	}
	enabledOrDisabled, found := c[n.StatementTag()]
	if !found {
		return false
	}
	if !enabledOrDisabled {
		panic(scerrors.NotImplementedErrorf(n,
			"statement has been disabled via cluster setting"))
	}
	return enabledOrDisabled
}

// GetSchemaChangerStatementControl returns a map of statements that
// are explicitly disabled by administrators for the declarative schema
// changer.
func getStatementsForceControl(sv *settings.Values) statementsForceControl {
	statements := forceDeclarativeStatements.Get(sv)
	var statementMap statementsForceControl
	for _, tag := range strings.Split(statements, ",") {
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
		if statementMap == nil {
			statementMap = make(statementsForceControl)
		}
		statementMap[tag] = enabledOrDisabled
	}
	return statementMap
}
