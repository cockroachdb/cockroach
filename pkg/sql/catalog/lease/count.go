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

	"github.com/cockroachdb/cockroach/pkg/keys"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
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
	codec keys.SQLCodec,
	cachedDatabaseRegions regionliveness.CachedDatabaseRegions,
	settings *clustersettings.Settings,
	versions []IDVersion,
	at hlc.Timestamp,
	forAnyVersion bool,
) (int, error) {
	useSyntheticDescriptors := useSyntheticLeaseDescriptor(settings)
	leasingMode := readSessionBasedLeasingMode(ctx, settings)
	whereClauses := make([][]string, 2)
	for _, t := range versions {
		versionClause := ""
		if !forAnyVersion {
			versionClause = fmt.Sprintf("AND version = %d", t.Version)
		}
		whereClauses[0] = append(
			whereClauses[0],
			fmt.Sprintf(`("descID" = %d %s AND expiration > $1)`, t.ID, versionClause),
		)
		whereClauses[1] = append(whereClauses[1],
			fmt.Sprintf(`(desc_id = %d %s AND (crdb_internal.sql_liveness_is_alive(session_id)))`,
				t.ID, versionClause),
		)
	}

	whereClauseIdx := make([]int, 0, 2)
	syntheticDescriptors := make(catalog.Descriptors, 0, 2)
	if leasingMode != SessionBasedOnly {
		if !useSyntheticDescriptors {
			syntheticDescriptors = append(syntheticDescriptors, systemschema.LeaseTable_V23_2())
		} else {
			syntheticDescriptors = append(syntheticDescriptors, nil)
		}
		whereClauseIdx = append(whereClauseIdx, 0)

	}
	if leasingMode >= SessionBasedDrain {
		if useSyntheticDescriptors {
			syntheticDescriptors = append(syntheticDescriptors, systemschema.LeaseTable())
		} else {
			syntheticDescriptors = append(syntheticDescriptors, nil)
		}
		whereClauseIdx = append(whereClauseIdx, 1)
	}

	var count int
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		txn.KV().SetDebugName("count-leases")
		if err := txn.KV().SetFixedTimestamp(ctx, at); err != nil {
			return err
		}
		prober := regionliveness.NewLivenessProber(db.KV(), codec, cachedDatabaseRegions, settings)
		regionMap, err := prober.QueryLiveness(ctx, txn.KV())
		if err != nil {
			return err
		}
		// Depending on the database configuration query by region or the
		// entire table.
		for i := range syntheticDescriptors {
			whereClause := whereClauses[whereClauseIdx[i]]
			var descsToInject catalog.Descriptors
			if syntheticDescriptors[i] != nil {
				descsToInject = append(descsToInject, syntheticDescriptors[i])
			}
			err := txn.WithSyntheticDescriptors(descsToInject,
				func() error {
					var err error
					if cachedDatabaseRegions != nil && cachedDatabaseRegions.IsMultiRegion() {
						// If we are injecting a raw leases descriptors, that will not have the enum
						// type set, so convert the region to byte equivalent physical representation.
						count, err = countLeasesByRegion(ctx, txn, prober, regionMap, cachedDatabaseRegions, len(descsToInject) > 0, at, whereClause)
					} else {
						count, err = countLeasesNonMultiRegion(ctx, txn, at, whereClause)
					}
					return err
				})
			if err != nil {
				return err
			}
			// Exit if either the session or expiry based counts are zero.
			if count > 0 {
				return nil
			}
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
		`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE 
crdb_region=$2 AND`,
		at.AsOfSystemTime(),
	) + strings.Join(whereClauses, " OR ")
	values, err := txn.QueryRowEx(
		ctx, "count-leases", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		stmt,
		at.GoTime(),
		enum.One, // Single region database can only have one region prefix assigned.
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
	cachedDBRegions regionliveness.CachedDatabaseRegions,
	convertRegionsToBytes bool,
	at hlc.Timestamp,
	whereClauses []string,
) (int, error) {
	regionClause := "crdb_region=$2::system.crdb_internal_region"
	if convertRegionsToBytes {
		regionClause = "crdb_region=$2"
	}
	stmt := fmt.Sprintf(
		`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE `,
		at.AsOfSystemTime(),
	) + regionClause + ` AND (` + strings.Join(whereClauses, " OR ") + ")"
	var count int
	if err := regionMap.ForEach(func(region string) error {
		regionEnumValue := region
		// The leases table descriptor injected does not have the type of the column
		// set to the region enum type. So, instead convert the logical value to
		// the physical one for comparison.
		// TODO(fqazi): In 24.2 when this table format is default we can stop using
		// synthetic descriptors and use the first code path.
		if convertRegionsToBytes {
			regionTypeDesc := cachedDBRegions.GetRegionEnumTypeDesc().AsRegionEnumTypeDescriptor()
			for i := 0; i < regionTypeDesc.NumEnumMembers(); i++ {
				if regionTypeDesc.GetMemberLogicalRepresentation(i) == region {
					regionEnumValue = string(regionTypeDesc.GetMemberPhysicalRepresentation(i))
					break
				}
			}
		}
		var values tree.Datums
		queryRegionRows := func(countCtx context.Context) error {
			var err error
			values, err = txn.QueryRowEx(
				countCtx, "count-leases", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				stmt, at.GoTime(), regionEnumValue,
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
