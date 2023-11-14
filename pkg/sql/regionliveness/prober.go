// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package regionliveness

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var RegionLivenessEnabled = settings.RegisterBoolSetting(settings.ApplicationLevel,
	"sql.region_liveness.enabled",
	"enables region liveness for system databases (experimental)",
	false, /* disabled */
	settings.WithVisibility(settings.Reserved))

// LiveRegions are regions which are currently still avaialble,
// and not quarantined due to expiration.
type LiveRegions map[string]struct{}

// ForEach does ordered iteration over the regions.
func (l LiveRegions) ForEach(fn func(region string) error) error {
	regions := make([]string, 0, len(l))
	for r := range l {
		regions = append(regions, r)
	}
	sort.Slice(regions, func(a, b int) bool {
		return regions[a] < regions[b]
	})
	for _, r := range regions {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

// Prober used to determine the set of regions which are still alive.
type Prober interface {
	// ProbeLiveness can be used after a timeout to label a regions as unavailable.
	ProbeLiveness(ctx context.Context, region string) error
	// QueryLiveness can be used to get the list of regions which are currently
	// accessible.
	QueryLiveness(ctx context.Context, txn *kv.Txn) (LiveRegions, error)
	// GetTableTimeout gets maximum timeout waiting on a table before issuing
	//// liveness queries.
	GetTableTimeout() (bool, time.Duration)
}

// RegionProvider abstracts the lookup of regions (see regions.Provider).
type RegionProvider interface {
	// GetRegions provides access to the set of regions available to the
	// current tenant.
	GetRegions(ctx context.Context) (*serverpb.RegionsResponse, error)
}

type CachedDatabaseRegions interface {
	IsMultiRegion() bool
	GetRegionEnumTypeDesc() catalog.RegionEnumTypeDescriptor
}

type livenessProber struct {
	db              isql.DB
	cachedDBRegions CachedDatabaseRegions
	settings        *clustersettings.Settings
}

var probeLivenessTimeout = 15 * time.Second

func TestingSetProbeLivenessTimeout(newTimeout time.Duration) func() {
	oldTimeout := probeLivenessTimeout
	probeLivenessTimeout = newTimeout
	return func() {
		probeLivenessTimeout = oldTimeout
	}
}

// NewLivenessProber creates a new region liveness prober.
func NewLivenessProber(
	db isql.DB, cachedDBRegions CachedDatabaseRegions, settings *clustersettings.Settings,
) Prober {
	return &livenessProber{
		db:              db,
		cachedDBRegions: cachedDBRegions,
		settings:        settings,
	}
}

// ProbeLiveness implements Prober.
func (l *livenessProber) ProbeLiveness(ctx context.Context, region string) error {
	// If region liveness is disabled then nothing to do.
	if !RegionLivenessEnabled.Get(&l.settings.SV) {
		return nil
	}
	const probeQuery = `
SELECT count(*) FROM system.sql_instances WHERE crdb_region = $1::system.crdb_internal_region
`
	err := timeutil.RunWithTimeout(ctx, "probe-liveness", probeLivenessTimeout,
		func(ctx context.Context) error {
			return l.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				_, err := txn.QueryRowEx(
					ctx, "probe-sql-instance", txn.KV(), sessiondata.NodeUserSessionDataOverride,
					probeQuery, region,
				)
				if err != nil {
					return err
				}
				return nil
			})
		})

	// Region is alive or we hit some other error.
	if err == nil || !IsQueryTimeoutErr(err) {
		return err
	}

	// Region has gone down, set the unavailable_at time on it
	return l.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		defaultTTL := slinstance.DefaultTTL.Get(&l.settings.SV)
		defaultHeartbeat := slinstance.DefaultHeartBeat.Get(&l.settings.SV)
		// Get the read timestamp and pick a commit deadline.
		readTS := txn.KV().ReadTimestamp().AddDuration(defaultHeartbeat)
		txnTS := readTS.AddDuration(defaultTTL)
		// Confirm that unavailable_at is not already set.
		rows, err := txn.QueryRow(ctx, "check-region-unavailable-exists", txn.KV(),
			"SELECT unavailable_at FROM system.region_liveness WHERE unavailable_at IS NOT NULL AND crdb_region=$1",
			region)
		// If there is any row from this query then unavailable_at is already set,
		// so don't push it further.
		if err != nil || len(rows) == 1 {
			return err
		}
		// Upset a new unavailable_at time.
		_, err = txn.Exec(ctx, "mark-region-unavailable", txn.KV(),
			"UPSERT into system.region_liveness(crdb_region, unavailable_at) VALUES ($1, $2)",
			region,
			txnTS.GoTime())
		if err != nil {
			return err
		}
		// Transaction has moved the read timestamp forward,
		// so force a retry.
		if txn.KV().ReadTimestamp().After(readTS) {
			return txn.KV().GenerateForcedRetryableErr(ctx, "read timestamp has moved unable to set deadline.")
		}
		return txn.KV().UpdateDeadline(ctx, readTS)
	})

}

// QueryLiveness implements Prober.
func (l *livenessProber) QueryLiveness(ctx context.Context, txn *kv.Txn) (LiveRegions, error) {
	executor := l.db.Executor()
	// Database is not multi-region so report a single region.
	if l.cachedDBRegions == nil ||
		!l.cachedDBRegions.IsMultiRegion() {
		return nil, nil
	}
	regionStatus := make(LiveRegions)
	if err := l.cachedDBRegions.GetRegionEnumTypeDesc().ForEachPublicRegion(func(regionName catpb.RegionName) error {
		regionStatus[string(regionName)] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}
	// If region liveness is disabled, return nil.
	if !RegionLivenessEnabled.Get(&l.settings.SV) {
		return regionStatus, nil
	}
	// Detect and down regions and remove them.
	rows, err := executor.QueryBufferedEx(
		ctx, "query-region-liveness", txn, sessiondata.NodeUserSessionDataOverride,
		"SELECT crdb_region, unavailable_at FROM system.region_liveness",
	)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		enum, _ := tree.AsDEnum(row[0])
		unavailableAt := tree.MustBeDTimestamp(row[1])
		// Region is now officially unavailable, so lets remove
		// it.
		if txn.ReadTimestamp().GoTime().After(unavailableAt.Time) {
			delete(regionStatus, enum.LogicalRep)
		}
	}
	return regionStatus, nil
}

// GetTableTimeout gets maximum timeout waiting on a table before issuing
// liveness queries.
func (l *livenessProber) GetTableTimeout() (bool, time.Duration) {
	return RegionLivenessEnabled.Get(&l.settings.SV), probeLivenessTimeout
}

// IsQueryTimeoutErr determines if a query timeout error was hit, specifically
// when checking for region liveness.
func IsQueryTimeoutErr(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.QueryCanceled ||
		errors.HasType(err, (*timeutil.TimeoutError)(nil))
}

// IsMissingRegionEnumErr determines if a query hit an error because of a missing
// because of the region enum.
func IsMissingRegionEnumErr(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.InvalidTextRepresentation
}
