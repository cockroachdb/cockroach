// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// DoBlock represents a SQL DO statement. It can only exist as a top-level
// statement.
type DoBlock struct {
	Code DoBlockBody
}

// DoBlockBody represents the fully parsed code body of a DO statement.
// It currently maps to plpgsqltree.DoBlock.
type DoBlockBody interface {
	NodeFormatter
	VisitBody(v Visitor) DoBlockBody
	IsDoBlockBody()
}

var _ Statement = &DoBlock{}

// Format implements the NodeFormatter interface.
func (n *DoBlock) Format(ctx *FmtCtx) {
	ctx.FormatNode(n.Code)
}

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
