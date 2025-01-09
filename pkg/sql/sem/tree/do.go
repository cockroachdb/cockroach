// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// DoBlockOptions represent a list of options for a DO statement.
type DoBlockOptions []DoBlockOption

// DoBlockOption is an interface representing DO statement properties.
type DoBlockOption interface {
	doBlockOption()
}

func (RoutineBodyStr) doBlockOption()  {}
func (RoutineLanguage) doBlockOption() {}

// AnalyzeDoBlockOptions checks the list of DO block options for validity, and
// returns the code string.
func AnalyzeDoBlockOptions(options DoBlockOptions) (RoutineBodyStr, error) {
	var doBody RoutineBodyStr
	var lang RoutineLanguage
	var foundLanguage, foundBody bool
	for _, doOption := range options {
		switch t := doOption.(type) {
		case RoutineBodyStr:
			if foundBody {
				return "", pgerror.New(pgcode.Syntax, "conflicting or redundant options")
			}
			doBody = t
			foundBody = true
		case RoutineLanguage:
			if foundLanguage {
				return "", pgerror.New(pgcode.Syntax, "conflicting or redundant options")
			}
			lang = t
			foundLanguage = true
		default:
			return "", errors.AssertionFailedf("unexpected DO statement option: %v", t)
		}
	}
	if !foundBody {
		return "", pgerror.New(pgcode.Syntax, "no inline code specified")
	}
	switch lang {
	case "", RoutineLangPLpgSQL:
		// PL/pgSQL is the default, and currently the only supported language.
	case RoutineLangSQL:
		// Postgres disallows SQL for DO statements.
		return "", pgerror.Newf(pgcode.FeatureNotSupported,
			"language \"%s\" does not support inline code execution", lang,
		)
	default:
		return "", pgerror.Newf(pgcode.UndefinedObject, "language \"%s\" does not exist", lang)
	}
	return doBody, nil
}
