// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// getSingleRegionClause returns an extra clause if the system database is in
// a single region to optimize scans on the system.leases table.
func getSingleRegionClause(
	ctx context.Context, isMultiRegion bool, st *cluster.Settings,
) (query string, queryParam []byte) {
	query, queryParam = "", nil
	if !isMultiRegion && st.Version.IsActive(ctx, clusterversion.V23_1_SystemRbrSingleWrite) {
		query = " AND crdb_region=$2"
		queryParam = enum.One
	}
	return query, queryParam
}

// CountLeases returns the number of unexpired leases for a number of descriptors
// each at a particular version at a particular time.
func CountLeases(
	ctx context.Context,
	executor isql.Executor,
	isMultiRegion bool,
	st *cluster.Settings,
	versions []IDVersion,
	at hlc.Timestamp,
) (int, error) {
	whereClauses := strings.Builder{}
	for i, t := range versions {
		if i != 0 {
			whereClauses.WriteString(" OR ")
		}
		whereClauses.WriteString(fmt.Sprintf(`("descID" = %d AND version = %d AND expiration > $1)`,
			t.ID, t.Version))
	}

	singleRegionClause, singleRegionParam := getSingleRegionClause(ctx, isMultiRegion, st)
	stmt := fmt.Sprintf(`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE (%s) %s `,
		at.AsOfSystemTime(),
		whereClauses.String(),
		singleRegionClause)

	values, err := executor.QueryRowEx(
		ctx, "count-leases", nil, /* txn */
		sessiondata.RootUserSessionDataOverride,
		stmt, at.GoTime(), singleRegionParam,
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
