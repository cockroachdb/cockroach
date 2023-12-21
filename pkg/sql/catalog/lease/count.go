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
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	var whereClauses []string
	for _, t := range versions {
		versionClause := ""
		if !forAnyVersion {
			versionClause = fmt.Sprintf("AND version = %d", t.Version)
		}
		whereClauses = append(
			whereClauses,
			fmt.Sprintf(`("descID" = %d %s AND expiration > $1)`, t.ID, versionClause),
		)
	}

	var count int
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		txn.KV().SetDebugName("count-leases")
		if err := txn.KV().SetFixedTimestamp(ctx, at); err != nil {
			return err
		}
		prober := regionliveness.NewLivenessProber(db, cachedDatabaseRegions, settings)
		regionMap, err := prober.QueryLiveness(ctx, txn.KV())
		if err != nil {
			return err
		}
		// Depending on the database configuration query by region or the
		// entire table.
		if cachedDatabaseRegions != nil && cachedDatabaseRegions.IsMultiRegion() {
			count, err = countLeasesByRegion(ctx, txn, prober, regionMap, at, whereClauses)
		} else {
			count, err = countLeasesNonMultiRegion(ctx, txn, at, whereClauses)
		}
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}

// Counts leases in non multi-region environments.
func countLeasesNonMultiRegion(
	ctx context.Context, txn isql.Txn, at hlc.Timestamp, whereClauses []string,
) (int, error) {
	stmt := fmt.Sprintf(
		`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE `,
		at.AsOfSystemTime(),
	) + strings.Join(whereClauses, " OR ")
	values, err := txn.QueryRowEx(
		ctx, "count-leases", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
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

// Counts leases by region in MR environments.
func countLeasesByRegion(
	ctx context.Context,
	txn isql.Txn,
	prober regionliveness.Prober,
	regionMap regionliveness.LiveRegions,
	at hlc.Timestamp,
	whereClauses []string,
) (int, error) {
	stmt := fmt.Sprintf(
		`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE `,
		at.AsOfSystemTime(),
	) + `crdb_region=$2::system.crdb_internal_region AND (` + strings.Join(whereClauses, " OR ") + ")"
	var count int
	if err := regionMap.ForEach(func(region string) error {
		var values tree.Datums
		queryRegionRows := func(countCtx context.Context) error {
			var err error
			values, err = txn.QueryRowEx(
				countCtx, "count-leases", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				stmt, at.GoTime(), region,
			)
			return err
		}
		var err error
		if hasTimeout, timeout := prober.GetProbeTimeout(); hasTimeout {
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
				log.Infof(ctx, "count-lease is skipping region %s because of the "+
					"following error %v", region, err)
				return nil
			}
			return err
		}
		if values == nil {
			return errors.New("failed to count leases")
		}
		count += int(tree.MustBeDInt(values[0]))
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}
