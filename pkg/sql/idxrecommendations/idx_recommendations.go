// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxrecommendations

import (
	"fmt"
	"strings"

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
		recommendations []string,
		reset bool,
	) []string
}

// FormatIdxRecommendations received a list with recommendations info, e.g.:
//{
//	"index recommendations: 2",
//	"1. type: index replacement",
//	"SQL commands: CREATE UNIQUE INDEX ON t1 (i) STORING (k); DROP INDEX t1@existing_t1_i;",
//  "2. type: index creation",
//  "SQL command: CREATE INDEX ON t2 (i) STORING (k);",
//}
// and returns a list of type and recommendations, e.g.:
//{
//		"replacement : CREATE UNIQUE INDEX ON t1 (i) STORING (k); DROP INDEX t1@existing_t1_i;",
//    "creation : CREATE INDEX ON t2 (i) STORING (k);",
//}
func FormatIdxRecommendations(idxRec []string) []string {
	recommendations := []string{}
	if len(idxRec) == 0 {
		return recommendations
	}

	var recType string
	var recCommand string
	var rec string

	for i := 1; i < len(idxRec); i++ {
		recType = strings.Split(idxRec[i], "type: index ")[1]
		recCommand = strings.Replace(idxRec[i+1], "   SQL command: ", "", 1)
		recCommand = strings.Replace(recCommand, "   SQL commands: ", "", 1)
		rec = fmt.Sprintf("%s : %s", recType, recCommand)
		recommendations = append(recommendations, rec)
		i++
	}

	return recommendations
}
