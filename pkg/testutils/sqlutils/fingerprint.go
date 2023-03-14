// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	"fmt"
	"testing"
)

func FingerprintTable(t testing.TB, sqlDB *SQLRunner, tableID uint32) int {
	fingerprintQuery := fmt.Sprintf(`
SELECT *
FROM
	crdb_internal.fingerprint(
		crdb_internal.table_span(%d),
		true
	)`, tableID)

	var fingerprint int
	sqlDB.QueryRow(t, fingerprintQuery).Scan(&fingerprint)
	return fingerprint
}
