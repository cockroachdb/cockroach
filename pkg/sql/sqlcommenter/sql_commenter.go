// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlcommenter

import (
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/redact"
)

var sqlCommenterEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.sqlcommenter.enabled",
	"enables support for sqlcommenter. Key value parsed from sqlcommenter "+
		"comments will be included in sql insights and sql logs. "+
		"See https://google.github.io/sqlcommenter/ for more details.",
	false,
	settings.WithPublic)

type QueryTag struct {
	Key   string
	Value redact.SafeString
}

// ExtractQueryTags Parses the provided comment to extract sqlcommenter key value
// tags. If the comment is in the expected format, it returns a list of tags.
// Otherwise, it returns an empty list.
//
// The algorithm followed for parsing: https://google.github.io/sqlcommenter/spec/#parsing.
func ExtractQueryTags(comment string) []QueryTag {
	comment = strings.TrimSpace(comment)
	if strings.HasPrefix(comment, "--") {
		comment = comment[2:]
	} else if strings.HasPrefix(comment, "/*") && strings.HasSuffix(comment, "*/") {
		comment = comment[2 : len(comment)-2]
	}

	pairs := strings.Split(comment, ",")
	if len(pairs) == 0 {
		return nil
	}

	tags := make([]QueryTag, 0, len(pairs))
	sqlCommenterTags := make(map[string]struct{}, len(pairs))
	for _, pair := range pairs {
		elems := strings.Split(pair, "=")
		if len(elems) != 2 {
			return nil
		}
		key := normalize(strings.TrimSpace(elems[0]))
		if _, exists := sqlCommenterTags[key]; exists {
			continue
		}

		v := strings.TrimSpace(elems[1])
		// Values must be wrapped in single quotes. If not, consider it invalid
		// and return an empty map.
		if !strings.HasPrefix(v, "'") || !strings.HasSuffix(v, "'") {
			return nil
		}

		// Remove the wrapping single quotes and normalize the value.
		value := normalize(v[1 : len(v)-1])
		sqlCommenterTags[key] = struct{}{}
		tags = append(tags, QueryTag{key, redact.SafeString(value)})
	}

	return tags
}

func MaybeRetainComments(sv *settings.Values) parser.ParseOptions {
	if sqlCommenterEnabled.Get(sv) {
		return parser.DefaultParseOptions.RetainComments()
	}
	return parser.DefaultParseOptions
}

func normalize(s string) string {
	s = strings.Replace(s, "\\'", "'", -1)
	s, _ = url.QueryUnescape(s)
	s, _ = url.PathUnescape(s)
	return s
}
