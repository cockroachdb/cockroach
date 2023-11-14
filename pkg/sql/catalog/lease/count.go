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
	regionProviderFactory regionliveness.RegionProviderFactory,
	settings *clustersettings.Settings,
	versions []IDVersion,
	at hlc.Timestamp,
) (int, error) {
	var count = 0
	txn := db.KV().NewTxn(ctx, "count-lease")
	executor := db.Executor()
	err := txn.SetFixedTimestamp(ctx, at)
	if err != nil {
		return 0, err
	}
	prober := regionliveness.NewLivenessProber(db, regionProviderFactory, settings)
	regionMap, err := prober.QueryLiveness(ctx, txn)
	if err != nil {
		return 0, err
	}
	var whereClauses []string
	for _, t := range versions {
		whereClauses = append(whereClauses,
			fmt.Sprintf(`("descID" = %d AND version = %d AND expiration > $1)`,
				t.ID, t.Version),
		)
	}
	regionClause := "("
	isPotentialMRDB := len(regionMap) > 0
	isMultiRegionDB := false
	if isPotentialMRDB {
		isMultiRegionDB, err = regionliveness.IsMultiRegionSystemDB(ctx, executor, txn)
		if err != nil {
			return 0, err
		}
	}
	if isMultiRegionDB {
		regionClause = `crdb_region=$2::system.crdb_internal_region AND (`
	}
	stmt := fmt.Sprintf(`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE `,
		at.AsOfSystemTime()) +
		regionClause +
		strings.Join(whereClauses, " OR ") +
		")"

	if err := regionMap.ForEach(func(region string) error {
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
