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

	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/regionliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// CountLeases returns the number of unexpired leases for a number of descriptors
// each at a particular version at a particular time.
func CountLeases(
	ctx context.Context,
	db isql.DB,
	cachedDatabaseRegions regionliveness.CachedDatabaseRegions,
	settings *clustersettings.Settings,
	versions []IDVersion,
	at hlc.Timestamp,
	forAnyVersion bool,
) (int, error) {
	var count = 0
	txn := db.KV().NewTxn(ctx, "count-lease")
	executor := db.Executor()
	err := txn.SetFixedTimestamp(ctx, at)
	if err != nil {
		return 0, err
	}
	prober := regionliveness.NewLivenessProber(db, cachedDatabaseRegions, settings)
	regionMap, err := prober.QueryLiveness(ctx, txn)
	if err != nil {
		return 0, err
	}
	var whereClauses []string
	for _, t := range versions {
		versionClause := ""
		if !forAnyVersion {
			versionClause = fmt.Sprintf("AND version = %d", t.Version)
		}
		whereClauses = append(whereClauses,
			fmt.Sprintf(`("descID" = %d %s AND expiration > $1)`,
				t.ID, versionClause),
		)
	}

	// Counts leases in non multi-region environments.
	countLeases := func() error {
		stmt := fmt.Sprintf(`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE `,
			at.AsOfSystemTime()) +
			strings.Join(whereClauses, " OR ")
		var values tree.Datums
		values, err = executor.QueryRowEx(
			ctx, "count-leases", txn, /* txn */
			sessiondata.RootUserSessionDataOverride,
			stmt, at.GoTime(),
		)
		if values == nil {
			return errors.New("failed to count leases")
		}
		count = int(tree.MustBeDInt(values[0]))
		return nil
	}

	// Counts leases by region in MR environments.
	countLeasesByRegion := func() error {
		stmt := fmt.Sprintf(`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE `,
			at.AsOfSystemTime()) +
			`crdb_region=$2::system.crdb_internal_region AND (` +
			strings.Join(whereClauses, " OR ") +
			")"
		return regionMap.ForEach(func(region string) error {
			var values tree.Datums
			queryRegionRows := func(countCtx context.Context) error {
				var err error
				values, err = executor.QueryRowEx(
					countCtx, "count-leases", txn, /* txn */
					sessiondata.RootUserSessionDataOverride,
					stmt, at.GoTime(), region,
				)
				return err
			}
			if hasTimeout, timeout := prober.GetTableTimeout(); hasTimeout {
				err = timeutil.RunWithTimeout(ctx, "count-leases-region", timeout, queryRegionRows)
			} else {
				err = queryRegionRows(ctx)
			}
			if err != nil {
				if regionliveness.IsQueryTimeoutErr(err) {
					// Probe and mark the region potentially.
					probeErr := prober.ProbeLiveness(ctx, region)
					if probeErr != nil {
						err = errors.WithSecondaryError(err, probeErr)
						return err
					}
					return errors.Wrapf(err, "count-lease timed out reading from a region")
				} else if regionliveness.IsMissingRegionEnumErr(err) {
					// Skip this region because we were unable to find region in
					// type descriptor. Since the database regions are cached, they
					// may be stale and have dropped regions.
					return nil
				}
				return err
			}
			if values == nil {
				return errors.New("failed to count leases")
			}
			count += int(tree.MustBeDInt(values[0]))
			return nil
		})
	}

	// Depending on the database configuration query by region or the
	// entire table.
	if cachedDatabaseRegions != nil &&
		cachedDatabaseRegions.IsMultiRegion() {
		err = countLeasesByRegion()
	} else {
		err = countLeases()
	}
	return count, err
}
