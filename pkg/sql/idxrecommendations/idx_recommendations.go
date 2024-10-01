// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package idxrecommendations

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/indexrec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// IdxRecommendations controls the generation of index recommendations
// for specific statements, and update accordingly.
type IdxRecommendations interface {
	ShouldGenerateIndexRecommendation(
		fingerprint string, planHash uint64, database string, stmtType tree.StatementType, isInternal bool,
	) bool
	UpdateIndexRecommendations(
		fingerprint string,
		planHash uint64,
		database string,
		stmtType tree.StatementType,
		isInternal bool,
		recommendations []indexrec.Rec,
		reset bool,
	) []indexrec.Rec
}

// FormatIdxRecommendations formats a list of index recommendations. The output
// is in the format:
//
//	{
//	  "replacement : CREATE UNIQUE INDEX ON t1 (i) STORING (k); DROP INDEX t1@existing_t1_i;",
//	  "creation : CREATE INDEX ON t2 (i) STORING (k);",
//	}
func FormatIdxRecommendations(recs []indexrec.Rec) []string {
	if len(recs) == 0 {
		return nil
	}
	recommendations := make([]string, len(recs))
	for i := range recs {
		recType := ""
		switch recs[i].RecType {
		case indexrec.TypeCreateIndex:
			recType = "creation"
		case indexrec.TypeReplaceIndex:
			recType = "replacement"
		case indexrec.TypeAlterIndex:
			recType = "alteration"
		}
		recommendations[i] = recType + " : " + recs[i].SQL
	}

	return recommendations
}
