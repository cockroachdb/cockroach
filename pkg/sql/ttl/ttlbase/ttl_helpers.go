// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ttlbase

import (
	"bytes"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
)

// DefaultAOSTDuration is the default duration to use in the AS OF SYSTEM TIME
// clause used in the SELECT query.
const DefaultAOSTDuration = -time.Second * 30

// SelectTemplate is the format string used to build SELECT queries for the
// TTL job.
const SelectTemplate = `SELECT %[1]s FROM [%[2]d AS tbl_name]
AS OF SYSTEM TIME %[3]s
WHERE %[4]s <= $1
%[5]s%[6]s
ORDER BY %[1]s
LIMIT %[7]v`

// DeleteTemplate is the format string used to build DELETE queries for the
// TTL job.
const DeleteTemplate = `DELETE FROM [%d AS tbl_name]
WHERE %s <= $1
AND (%s) IN (%s)`

// MakeColumnNamesSQL converts columns into an escape string
// for an order by clause, e.g.:
//   {"a", "b"} => a, b
//   {"escape-me", "b"} => "escape-me", b
func MakeColumnNamesSQL(columns []string) string {
	var b bytes.Buffer
	for i, pkColumn := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		lexbase.EncodeRestrictedSQLIdent(&b, pkColumn, lexbase.EncNoFlags)
	}
	return b.String()
}
