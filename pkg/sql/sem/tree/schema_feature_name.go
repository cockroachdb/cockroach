// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "strings"

// SchemaFeatureName feature name for a given statement, which can be used
// to detect via the feature check functions if the schema change is allowed.
type SchemaFeatureName string

// GetSchemaFeatureNameFromStmt takes a statement and converts it to a schema
// feature name, which can be enabled or disabled via a feature flag.
func GetSchemaFeatureNameFromStmt(stmt Statement) SchemaFeatureName {
	statementTag := stmt.StatementTag()
	statementInfo := strings.Split(statementTag, " ")

	switch stmt.(type) {
	case *CommentOnDatabase, *CommentOnSchema, *CommentOnTable,
		*CommentOnColumn, *CommentOnIndex, *CommentOnConstraint, *DropOwnedBy:
		return SchemaFeatureName(statementTag)
	}
	// Only grab the first two words (i.e. ALTER TABLE, etc..).
	if len(statementInfo) >= 2 {
		return SchemaFeatureName(statementInfo[0] + " " + statementInfo[1])
	}
	return SchemaFeatureName(statementInfo[0])
}
