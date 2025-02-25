// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Parsing log to parse the comments added by
// SQL commenter: https://google.github.io/sqlcommenter/spec/#parsing.
func extractSQLCommenterTags(comment string) (map[string]string, error) {
	comment = strings.TrimSpace(comment)
	comment = strings.Replace(comment, "--", "", 1)
	comment = strings.Replace(comment, "/*", "", 1)
	comment = strings.Replace(comment, "*/", "", 1)

	pairs := strings.Split(comment, ",")
	if len(pairs) == 0 {
		return nil, fmt.Errorf("error parsing SQL comment")
	}

	normalize := func(s string) string {
		if us, err := strconv.Unquote(s); err == nil {
			s = us
		}
		s, _ = url.QueryUnescape(s)
		s, _ = url.PathUnescape(s)
		return s
	}

	sqlCommenterTags := make(map[string]string)
	for _, pair := range pairs {
		elems := strings.Split(pair, "=")
		if len(elems) != 2 {
			return nil, fmt.Errorf("error parsing SQL comment")
		}
		key := normalize(strings.TrimSpace(elems[0]))
		value := normalize(strings.Trim(strings.TrimSpace(elems[1]), "'"))
		sqlCommenterTags[key] = value
	}

	return sqlCommenterTags, nil
}

// buildSqlCommenterTagsStr builds the comment string from the commenter tags
// using spec outlined in https://google.github.io/sqlcommenter/spec/#parsing.
func buildSqlCommenterTagsStr(sqlCommenterTags map[string]string) string {
	b := strings.Builder{}
	for k, v := range sqlCommenterTags {
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(v)
		b.WriteString(";")
	}
	return b.String()
}
