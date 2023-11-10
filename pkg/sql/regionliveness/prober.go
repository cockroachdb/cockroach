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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

// Prober used to determine the set of regions which are still alive.
type Prober interface {
	// ProbeLiveness can be used after a timeout to label a regions as unavailable.
	ProbeLiveness(ctx context.Context, region string) error
	// QueryLiveness can be used to get the list of regions which are currently
	// accessible.
	QueryLiveness(ctx context.Context, txn *kv.Txn) (LiveRegions, error)
}

// RegionProvider abstracts the lookup of regions (see regions.Provider).
type RegionProvider interface {
	// GetRegions provides access to the set of regions available to the
	// current tenant.
	GetRegions(ctx context.Context) (*serverpb.RegionsResponse, error)
}

type RegionProviderFactory func(txn *kv.Txn) RegionProvider

type livenessProber struct {
	db                    isql.DB
	regionProviderFactory RegionProviderFactory
	settings              *clustersettings.Settings
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
	db isql.DB, regionProviderFactory RegionProviderFactory, settings *clustersettings.Settings,
) Prober {
	return &livenessProber{
		db:                    db,
		regionProviderFactory: regionProviderFactory,
		settings:              settings,
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
	regionStatus := make(LiveRegions)
	executor := l.db.Executor()
	regionProvider := l.regionProviderFactory(txn)
	regions, err := regionProvider.GetRegions(ctx)
	if err != nil {
		return nil, err
	}
	// Add entries for regions
	for region := range regions.Regions {
		regionStatus[region] = struct{}{}
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

// IsQueryTimeoutErr determines if a query timeout error was hit, specifically
// when checking for region liveness.
func IsQueryTimeoutErr(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.QueryCanceled ||
		errors.HasType(err, (*timeutil.TimeoutError)(nil))
}
