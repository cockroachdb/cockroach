// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// schemaStatementControl track if a statement tag is enabled or disabled
// forcefully by the user.
type schemaStatementControl map[string]bool

// schemaChangerDisabledStatements statements which are disabled
// for the declarative schema changer. Users can specify statement
// tags for each statement and a "!" symbol in front can have the opposite
// effect to force enable fully unimplemented features.
var schemaChangerDisabledStatements = settings.RegisterStringSetting(
	settings.TenantWritable,
	"sql.schema.force_declarative_statements",
	"allows force enabling / disabling declarative schema changer for specific statements",
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

// CheckStatementControl if a statement is forced to disabled or enabled. If a
// statement is disabled then an not implemented error will be panicked. Otherwise,
// a flag is returned indicating if this statement has been *forced* to be enabled.
func (c schemaStatementControl) CheckStatementControl(n tree.Statement) (forceEnabled bool) {
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
func getSchemaChangerStatementControl(sv *settings.Values) schemaStatementControl {
	statements := schemaChangerDisabledStatements.Get(sv)
	var statementMap schemaStatementControl
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
			statementMap = make(schemaStatementControl)
		}
		statementMap[tag] = enabledOrDisabled
	}
	return statementMap
}
