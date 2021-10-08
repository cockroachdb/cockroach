// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package reports

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// replicationStatsReportID is the id of the row in the system. reports_meta
// table corresponding to the replication stats report (i.e. the
// system.replication_stats table).
const replicationStatsReportID reportID = 3

// RangeReport represents the system.zone_range_status report.
type RangeReport map[ZoneKey]zoneRangeStatus

// zoneRangeStatus is the leaf of the RangeReport.
type zoneRangeStatus struct {
	numRanges       int32
	unavailable     int32
	underReplicated int32
	overReplicated  int32
}

// replicationStatsReportSaver deals with saving a RangeReport to the database.
// The idea is for it to be used to save new version of the report over and
// over. It maintains the previously-saved version of the report in order to
// speed-up the saving of the next one.
type replicationStatsReportSaver struct {
	previousVersion     RangeReport
	lastGenerated       time.Time
	lastUpdatedRowCount int
}

// makeReplicationStatsReportSaver creates a new report saver.
func makeReplicationStatsReportSaver() replicationStatsReportSaver {
	return replicationStatsReportSaver{}
}

// LastUpdatedRowCount is the count of the rows that were touched during the last save.
func (r *replicationStatsReportSaver) LastUpdatedRowCount() int {
	return r.lastUpdatedRowCount
}

// EnsureEntry creates an entry for the given key if there is none.
func (r RangeReport) EnsureEntry(zKey ZoneKey) {
	if _, ok := r[zKey]; !ok {
		r[zKey] = zoneRangeStatus{}
	}
}

// CountRange adds one range's info to the report. If there's no entry in the
// report for the range's zone, a new one is created.
func (r RangeReport) CountRange(zKey ZoneKey, status roachpb.RangeStatusReport) {
	r.EnsureEntry(zKey)
	rStat := r[zKey]
	rStat.numRanges++
	if !status.Available {
		rStat.unavailable++
	}
	if status.UnderReplicated {
		rStat.underReplicated++
	}
	if status.OverReplicated {
		rStat.overReplicated++
	}
	r[zKey] = rStat
}

func (r *replicationStatsReportSaver) loadPreviousVersion(
	ctx context.Context, ex sqlutil.InternalExecutor, txn *kv.Txn,
) error {
	// The data for the previous save needs to be loaded if:
	// - this is the first time that we call this method and lastUpdatedAt has never been set
	// - in case that the lastUpdatedAt is set but is different than the timestamp in reports_meta
	//   this indicates that some other worker wrote after we did the write.
	if !r.lastGenerated.IsZero() {
		generated, err := getReportGenerationTime(ctx, replicationStatsReportID, ex, txn)
		if err != nil {
			return err
		}
		// If the report is missing, this is the first time we are running and the
		// reload is needed. In that case, generated will be the zero value.
		if generated == r.lastGenerated {
			// We have the latest report; reload not needed.
			return nil
		}
	}
	const prevViolations = "select zone_id, subzone_id, total_ranges, " +
		"unavailable_ranges, under_replicated_ranges, over_replicated_ranges " +
		"from system.replication_stats"
	it, err := ex.QueryIterator(
		ctx, "get-previous-replication-stats", txn, prevViolations,
	)
	if err != nil {
		return err
	}

	r.previousVersion = make(RangeReport)
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		key := ZoneKey{}
		key.ZoneID = (config.SystemTenantObjectID)(*row[0].(*tree.DInt))
		key.SubzoneID = base.SubzoneID(*row[1].(*tree.DInt))
		r.previousVersion[key] = zoneRangeStatus{
			(int32)(*row[2].(*tree.DInt)),
			(int32)(*row[3].(*tree.DInt)),
			(int32)(*row[4].(*tree.DInt)),
			(int32)(*row[5].(*tree.DInt)),
		}
	}
	return err
}

func (r *replicationStatsReportSaver) updateTimestamp(
	ctx context.Context, ex sqlutil.InternalExecutor, txn *kv.Txn, reportTS time.Time,
) error {
	if !r.lastGenerated.IsZero() && reportTS == r.lastGenerated {
		return errors.Errorf(
			"The new time %s is the same as the time of the last update %s",
			reportTS.String(),
			r.lastGenerated.String(),
		)
	}

	_, err := ex.Exec(
		ctx,
		"timestamp-upsert-replication-stats",
		txn,
		"upsert into system.reports_meta(id, generated) values($1, $2)",
		replicationStatsReportID,
		reportTS,
	)
	return err
}

// Save a report in the database.
//
// report should not be used by the caller any more after this call; the callee
// takes ownership.
// reportTS is the time that will be set in the updated_at column for every row.
func (r *replicationStatsReportSaver) Save(
	ctx context.Context,
	report RangeReport,
	reportTS time.Time,
	db *kv.DB,
	ex sqlutil.InternalExecutor,
) error {
	r.lastUpdatedRowCount = 0
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := r.loadPreviousVersion(ctx, ex, txn)
		if err != nil {
			return err
		}

		err = r.updateTimestamp(ctx, ex, txn, reportTS)
		if err != nil {
			return err
		}

		for key, status := range report {
			if err := r.upsertStats(ctx, txn, key, status, ex); err != nil {
				return err
			}
		}

		for key := range r.previousVersion {
			if _, ok := report[key]; !ok {
				_, err := ex.Exec(
					ctx,
					"delete-old-replication-stats",
					txn,
					"delete from system.replication_stats "+
						"where zone_id = $1 and subzone_id = $2",
					key.ZoneID,
					key.SubzoneID,
				)

				if err != nil {
					return err
				}
				r.lastUpdatedRowCount++
			}
		}

		return nil
	}); err != nil {
		return err
	}

	r.lastGenerated = reportTS
	r.previousVersion = report

	return nil
}

// upsertStat upserts a row into system.replication_stats.
func (r *replicationStatsReportSaver) upsertStats(
	ctx context.Context, txn *kv.Txn, key ZoneKey, stats zoneRangeStatus, ex sqlutil.InternalExecutor,
) error {
	var err error
	previousStats, hasOldVersion := r.previousVersion[key]
	if hasOldVersion && previousStats == stats {
		// No change in the stats so no update.
		return nil
	}

	// Updating an old row.
	_, err = ex.Exec(
		ctx, "upsert-replication-stats", txn,
		"upsert into system.replication_stats(report_id, zone_id, subzone_id, "+
			"total_ranges, unavailable_ranges, under_replicated_ranges, "+
			"over_replicated_ranges) values($1, $2, $3, $4, $5, $6, $7)",
		replicationStatsReportID,
		key.ZoneID, key.SubzoneID, stats.numRanges, stats.unavailable,
		stats.underReplicated, stats.overReplicated,
	)

	if err != nil {
		return err
	}

	r.lastUpdatedRowCount++
	return nil
}

// replicationStatsVisitor is a visitor that builds a RangeReport.
type replicationStatsVisitor struct {
	cfg         *config.SystemConfig
	nodeChecker nodeChecker

	// report is the output of the visitor. visit*() methods populate it.
	// After visiting all the ranges, it can be retrieved with Report().
	report   RangeReport
	visitErr bool

	// prevZoneKey and prevNumReplicas maintain state from one range to the next.
	// This state can be reused when a range is covered by the same zone config as
	// the previous one. Reusing it speeds up the report generation.
	prevZoneKey     ZoneKey
	prevNumReplicas int
}

var _ rangeVisitor = &replicationStatsVisitor{}

func makeReplicationStatsVisitor(
	ctx context.Context, cfg *config.SystemConfig, nodeChecker nodeChecker,
) replicationStatsVisitor {
	v := replicationStatsVisitor{
		cfg:         cfg,
		nodeChecker: nodeChecker,
		report:      make(RangeReport),
	}
	v.reset(ctx)
	return v
}

// failed is part of the rangeVisitor interface.
func (v *replicationStatsVisitor) failed() bool {
	return v.visitErr
}

// Report returns the RangeReport that was populated by previous visit*() calls.
func (v *replicationStatsVisitor) Report() RangeReport {
	return v.report
}

// reset is part of the rangeVisitor interface.
func (v *replicationStatsVisitor) reset(ctx context.Context) {
	*v = replicationStatsVisitor{
		cfg:             v.cfg,
		nodeChecker:     v.nodeChecker,
		prevNumReplicas: -1,
		report:          make(RangeReport, len(v.report)),
	}

	// Iterate through all the zone configs to create report entries for all the
	// zones that have constraints. Otherwise, just iterating through the ranges
	// wouldn't create entries for zones that don't apply to any ranges.
	maxObjectID, err := v.cfg.GetLargestObjectID(
		0 /* maxID - return the largest ID in the config */, keys.PseudoTableIDs,
	)
	if err != nil {
		log.Fatalf(ctx, "unexpected failure to compute max object id: %s", err)
	}
	for i := config.SystemTenantObjectID(1); i <= maxObjectID; i++ {
		zone, err := getZoneByID(i, v.cfg)
		if err != nil {
			log.Fatalf(ctx, "unexpected failure to compute max object id: %s", err)
		}
		if zone == nil {
			continue
		}
		v.ensureEntries(MakeZoneKey(i, NoSubzone), zone)
	}
}

func (v *replicationStatsVisitor) ensureEntries(key ZoneKey, zone *zonepb.ZoneConfig) {
	if zoneChangesReplication(zone) {
		v.report.EnsureEntry(key)
	}
	for i, sz := range zone.Subzones {
		v.ensureEntries(MakeZoneKey(key.ZoneID, base.SubzoneIDFromIndex(i)), &sz.Config)
	}
}

// visitNewZone is part of the rangeVisitor interface.
func (v *replicationStatsVisitor) visitNewZone(
	ctx context.Context, r *roachpb.RangeDescriptor,
) (retErr error) {

	defer func() {
		v.visitErr = retErr != nil
	}()
	var zKey ZoneKey
	var zConfig *zonepb.ZoneConfig
	var numReplicas int

	// Figure out the zone config for whose report the current range is to be
	// counted. This is the lowest-level zone config covering the range that
	// changes replication settings. We also need to figure out the replication
	// factor this zone is configured with; the replication factor might be
	// inherited from a higher-level zone config.
	found, err := visitZones(ctx, r, v.cfg, ignoreSubzonePlaceholders,
		func(_ context.Context, zone *zonepb.ZoneConfig, key ZoneKey) bool {
			if zConfig == nil {
				if !zoneChangesReplication(zone) {
					return false
				}
				zKey = key
				zConfig = zone
				if zone.NumReplicas != nil {
					numReplicas = int(*zone.NumReplicas)
					return true
				}
				// We need to continue upwards in search for the NumReplicas.
				return false
			}
			// We had already found the zone to report to, but we're haven't found
			// its NumReplicas yet.
			if zone.NumReplicas != nil {
				numReplicas = int(*zone.NumReplicas)
				return true
			}
			return false
		})
	if err != nil {
		return errors.AssertionFailedf("unexpected error visiting zones for range %s: %s", r, err)
	}
	v.prevZoneKey = zKey
	v.prevNumReplicas = numReplicas
	if !found {
		return errors.AssertionFailedf(
			"no zone config with replication attributes found for range: %s", r)
	}

	v.countRange(ctx, zKey, numReplicas, r)
	return nil
}

// visitSameZone is part of the rangeVisitor interface.
func (v *replicationStatsVisitor) visitSameZone(ctx context.Context, r *roachpb.RangeDescriptor) {
	v.countRange(ctx, v.prevZoneKey, v.prevNumReplicas, r)
}

func (v *replicationStatsVisitor) countRange(
	ctx context.Context, key ZoneKey, replicationFactor int, r *roachpb.RangeDescriptor,
) {
	status := r.Replicas().ReplicationStatus(func(rDesc roachpb.ReplicaDescriptor) bool {
		return v.nodeChecker(rDesc.NodeID)
	}, replicationFactor)
	// Note that a range can be under-replicated and over-replicated at the same
	// time if it has many replicas, but sufficiently many of them are on dead
	// nodes.
	v.report.CountRange(key, status)
}

// zoneChangesReplication determines whether a given zone config changes
// replication attributes: the replication factor or the replication
// constraints.
// This is used to determine which zone's report a range counts towards for the
// replication_stats and the critical_localities reports : it'll count towards
// the lowest ancestor for which this method returns true.
func zoneChangesReplication(zone *zonepb.ZoneConfig) bool {
	return (zone.NumReplicas != nil && *zone.NumReplicas != 0) ||
		zone.Constraints != nil
}
