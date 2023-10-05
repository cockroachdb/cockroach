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
	"github.com/cockroachdb/cockroach/pkg/settings"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

type livenessProber struct {
	db       isql.DB
	settings *clustersettings.Settings
}

// NewLivenessProber creates a new region liveness prober.
func NewLivenessProber(db isql.DB, settings *clustersettings.Settings) Prober {
	return &livenessProber{
		db:       db,
		settings: settings,
	}
}

// fetchSystemDBRegions fetches the regions that are added in the system database.
func (l *livenessProber) fetchSystemDBRegions(
	ctx context.Context, executor isql.Executor, txn *kv.Txn,
) ([]string, error) {
	const fetchDbRegions = `
SELECT
    regions 
FROM system.crdb_internal.databases
WHERE name='system'
`
	row, err := executor.QueryRow(ctx, "fetch-db-regions", txn, fetchDbRegions)
	if err != nil {
		return nil, err
	}
	regionDatums := tree.MustBeDArray(row[0])

	regions := make([]string, 0, len(regionDatums.Array))
	for _, region := range regionDatums.Array {
		regions = append(regions, string(tree.MustBeDString(region)))
	}
	return regions, nil
}

// ProbeLiveness implements Prober.
func (l *livenessProber) ProbeLiveness(ctx context.Context, region string) error {
	// If region liveness is disabled then nothing to do.
	if !RegionLivenessEnabled.Get(&l.settings.SV) {
		return nil
	}
	const probeQuery = `
SELECT * FROM system.sql_instances WHERE crdb_region=$1::system.crdb_internal_region
`
	err := timeutil.RunWithTimeout(ctx, "probe-liveness", time.Second*15,
		func(ctx context.Context) error {
			return l.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				_, err := txn.Exec(ctx, "probe-sql-instance", txn.KV(), probeQuery, region)
				return err
			})
		})

	// Region is alive or we hit some other error.
	if err == nil ||
		!IsQueryTimeoutErr(err) {
		return err
	}

	// Region has gone down, set the unavailable_at time on it
	return l.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var defaultTTL = time.Second * 30
		if l.settings != nil {
			defaultTTL = slinstance.DefaultTTL.Get(&l.settings.SV)
		}
		txnTS := txn.KV().ReadTimestamp().GoTime()
		txnTS = txnTS.Add(defaultTTL)

		_, err := txn.Exec(ctx, "mark-region-unavailable", txn.KV(),
			"UPSERT into system.region_liveness(crdb_region, unavailable_at) VALUES ($1, $2)",
			region,
			txnTS)
		return err
	})
}

// QueryLiveness implements Prober.
func (l *livenessProber) QueryLiveness(ctx context.Context, txn *kv.Txn) (LiveRegions, error) {
	// If region liveness is disabled, return nil.
	if !RegionLivenessEnabled.Get(&l.settings.SV) {
		return nil, nil
	}
	regionStatus := make(LiveRegions)
	executor := l.db.Executor()
	regions, err := l.fetchSystemDBRegions(ctx, executor, txn)
	if err != nil {
		return nil, err
	}
	// Add entries for regions
	for _, region := range regions {
		regionStatus[region] = struct{}{}
	}

	// Detect and down regions and remove them.
	rows, err := executor.QueryBuffered(ctx, "query-region-liveness", txn,
		"SELECT * FROM system.region_liveness")
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		enum, _ := tree.AsDEnum(row[0])
		timestamp := tree.MustBeDTimestamp(row[1])
		// Region is now officially unavailable, so lets remove
		// it.
		if txn.ReadTimestamp().GoTime().After(timestamp.Time) {
			delete(regionStatus, enum.LogicalRep)
		}
	}
	return regionStatus, nil
}

// IsQueryTimeoutErr determines if a query timeout error was hit, specifically
// when checking for region liveness.
func IsQueryTimeoutErr(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.QueryCanceled ||
		errors.Is(err, &timeutil.TimeoutError{})
}
