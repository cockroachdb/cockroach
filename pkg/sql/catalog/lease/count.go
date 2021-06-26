// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lease

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// CountLeases returns the number of unexpired leases for a number of descriptors
// each at a particular version at a particular time.
func CountLeases(
	ctx context.Context, executor sqlutil.InternalExecutor, versions []IDVersion, at hlc.Timestamp,
) (int, error) {
	var whereClauses []string
	for _, t := range versions {
		whereClauses = append(whereClauses,
			fmt.Sprintf(`("descID" = %d AND version = %d AND expiration > $1)`,
				t.ID, t.Version),
		)
	}

	stmt := fmt.Sprintf(`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE `,
		at.AsOfSystemTime()) +
		strings.Join(whereClauses, " OR ")
	values, err := executor.QueryRowEx(
		ctx, "count-leases", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		stmt, at.GoTime(),
	)
	if err != nil {
		return 0, err
	}
	if values == nil {
		return 0, errors.New("failed to count leases")
	}
	count := int(tree.MustBeDInt(values[0]))
	return count, nil
}
