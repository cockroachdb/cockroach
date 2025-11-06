// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlcommentersettings

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

var sqlCommenterEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.sqlcommenter.enabled",
	"enables support for sqlcommenter. Key value parsed from sqlcommenter "+
		"comments will be included in sql insights and sql logs. "+
		"See https://google.github.io/sqlcommenter/ for more details.",
	false,
	settings.WithPublic)

func MaybeRetainComments(sv *settings.Values) parser.ParseOptions {
	if sqlCommenterEnabled.Get(sv) {
		return parser.DefaultParseOptions.RetainComments()
	}
	return parser.DefaultParseOptions
}
