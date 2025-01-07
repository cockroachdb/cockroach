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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/regionliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type countDetail struct {
	// count is the number of unexpired leases
	count int
	// numSQLInstances is the number of distinct SQL instances with unexpired leases.
	numSQLInstances int
	// sampleSQLInstanceID is one of the sql_instance_id values we are waiting on,
	// but only if we are waiting on at least one lease. If the count is 0, this
	// value will also be 0.
	sampleSQLInstanceID int
}

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
	detail, err := countLeasesWithDetail(ctx, db, codec, cachedDatabaseRegions,
		settings, versions, at, forAnyVersion)
	if err != nil {
		return 0, err
	}
	return detail.count, nil
}

// isRegionColumnError detects if a InvalidParameterValue or
// UndefinedFunction are observed because of the region column.
// This can happen because of the following reasons:
//  1. The currently leased system database is not multi-region, but the leased
//     system.lease table is multi-region.
//  2. The currently leased system database is multi-region, but the system.lease
//     descriptor we have is not multi-region.
//
// Both cases are transient and are a side effect of using a cache system
// database descriptor for checks.
func isTransientRegionColumnError(err error) bool {
	// No error detected nothing else needs to be checked.
	if err == nil {
		return false
	}
	// Some unrelated error was observed, so this is not linked to the
	// region column transitioning from bytes to crdb_region.
	if pgerror.GetPGCode(err) != pgcode.UndefinedFunction &&
		pgerror.GetPGCode(err) != pgcode.InvalidParameterValue {
		return false
	}
	return strings.Contains(err.Error(), "crdb_internal_region")
}

func countLeasesWithDetail(
	ctx context.Context,
	db isql.DB,
	codec keys.SQLCodec,
	cachedDatabaseRegions regionliveness.CachedDatabaseRegions,
	settings *clustersettings.Settings,
	versions []IDVersion,
	at hlc.Timestamp,
	forAnyVersion bool,
) (countDetail, error) {
	// Indicates if the leasing descriptor has been upgraded for session based
	// leasing. Note: Unit tests will never provide cached database regions
	// so resolve the version from the cluster settings.
	var systemDBVersion *roachpb.Version
	if cachedDatabaseRegions != nil {
		systemDBVersion = cachedDatabaseRegions.GetSystemDatabaseVersion()
	} else {
		v := settings.Version.ActiveVersion(ctx).Version
		systemDBVersion = &v
	}
	leasingDescIsSessionBased := systemDBVersion != nil &&
		clusterversion.RemoveDevOffset(*systemDBVersion).AtLeast(
			clusterversion.RemoveDevOffset(clusterversion.V24_1_SessionBasedLeasingUpgradeDescriptor.Version()))
	leasingMode := readSessionBasedLeasingMode(ctx, settings)
	whereClauses := make([][]string, 2)
	forceMultiRegionQuery := false
	useBytesOnRetry := false
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
	usesOldSchema := make([]bool, 0, 2)
	syntheticDescriptors := make(catalog.Descriptors, 0, 2)
	if leasingMode != SessionBasedOnly {
		// The leasing descriptor is session based, so we need to inject
		// expiry based descriptor synthetically.
		if leasingDescIsSessionBased {
			syntheticDescriptors = append(syntheticDescriptors, systemschema.LeaseTable_V23_2())
		} else {
			syntheticDescriptors = append(syntheticDescriptors, nil)
		}
		whereClauseIdx = append(whereClauseIdx, 0)
		usesOldSchema = append(usesOldSchema, true)
	}
	if leasingMode >= SessionBasedDrain {
		// The leasing descriptor is not yet session based, so inject the session
		// based descriptor synthetically.
		if !leasingDescIsSessionBased {
			syntheticDescriptors = append(syntheticDescriptors, systemschema.LeaseTable())
		} else {
			syntheticDescriptors = append(syntheticDescriptors, nil)
		}
		whereClauseIdx = append(whereClauseIdx, 1)
		usesOldSchema = append(usesOldSchema, false)
	}

	var detail countDetail
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
					if (cachedDatabaseRegions != nil && cachedDatabaseRegions.IsMultiRegion()) ||
						forceMultiRegionQuery {
						// If we are injecting a raw leases descriptors, that will not have the enum
						// type set, so convert the region to byte equivalent physical representation.
						detail, err = countLeasesByRegion(ctx, txn, prober, regionMap, cachedDatabaseRegions,
							len(descsToInject) > 0 || useBytesOnRetry, at, whereClause, usesOldSchema[i])
					} else {
						detail, err = countLeasesNonMultiRegion(ctx, txn, at, whereClause, usesOldSchema[i])
					}
					// If any transient region column errors occur then we should retry the count query.
					if isTransientRegionColumnError(err) {
						forceMultiRegionQuery = true
						// If the query was already multi-region aware, then the system database is MR,
						// but our lease descriptor has not been upgraded yet.
						useBytesOnRetry = cachedDatabaseRegions != nil && cachedDatabaseRegions.IsMultiRegion()
						return txn.KV().GenerateForcedRetryableErr(ctx, "forcing retry once with MR columns")
					}
					return err
				})
			if err != nil {
				return err
			}
			// Exit if either the session or expiry based counts are zero.
			if detail.count > 0 {
				return nil
			}
		}
		return nil
	}); err != nil {
		return countDetail{}, err
	}
	return detail, nil
}

// Counts leases in non multi-region environments.
func countLeasesNonMultiRegion(
	ctx context.Context, txn isql.Txn, at hlc.Timestamp, whereClauses []string, usesOldSchema bool,
) (countDetail, error) {
	stmt := fmt.Sprintf(
		`SELECT %[1]s FROM system.public.lease AS OF SYSTEM TIME '%[2]s' WHERE 
crdb_region=$2 AND %[3]s`,
		getCountLeaseColumns(usesOldSchema),
		at.AsOfSystemTime(),
		strings.Join(whereClauses, " OR "),
	)
	values, err := txn.QueryRowEx(
		ctx, "count-leases", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		stmt,
		at.GoTime(),
		enum.One, // Single region database can only have one region prefix assigned.
	)
	if err != nil {
		return countDetail{}, err
	}
	if values == nil {
		return countDetail{}, errors.New("failed to count leases")
	}
	return countDetail{
		count:               int(tree.MustBeDInt(values[0])),
		numSQLInstances:     int(tree.MustBeDInt(values[1])),
		sampleSQLInstanceID: int(tree.MustBeDInt(values[2])),
	}, nil
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
	usesOldSchema bool,
) (countDetail, error) {
	regionClause := "crdb_region=$2::system.crdb_internal_region"
	if convertRegionsToBytes {
		regionClause = "crdb_region=$2"
	}
	stmt := fmt.Sprintf(
		`SELECT %[1]s FROM system.public.lease AS OF SYSTEM TIME '%[2]s' WHERE %[3]s `,
		getCountLeaseColumns(usesOldSchema),
		at.AsOfSystemTime(),
		regionClause+` AND (`+strings.Join(whereClauses, " OR ")+")",
	)
	var detail countDetail
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
		detail.count += int(tree.MustBeDInt(values[0]))
		detail.numSQLInstances += int(tree.MustBeDInt(values[1]))
		if detail.sampleSQLInstanceID == 0 { // only retain the first sample ID
			detail.sampleSQLInstanceID = int(tree.MustBeDInt(values[2]))
		}
		return nil
	}); err != nil {
		return countDetail{}, err
	}
	return detail, nil
}

func getCountLeaseColumns(usesOldSchema bool) string {
	var sb strings.Builder
	sb.WriteString("count(1)")
	// We only care about the count of leases, which is the first column. For
	// debugging purposes, we also return the number of distinct nodes we are
	// waiting on and one of the nodes we are waiting on. These two details will
	// appear in the periodic dumping of wait stats.
	//
	// The system.lease table went through some column renames in past versions.
	// Pick the correct version.
	if usesOldSchema {
		sb.WriteString(`, count(distinct "nodeID"), ifnull(min("nodeID"),0)`)
		return sb.String()
	}
	sb.WriteString(`, count(distinct sql_instance_id), ifnull(min(sql_instance_id),0)`)
	return sb.String()
}
