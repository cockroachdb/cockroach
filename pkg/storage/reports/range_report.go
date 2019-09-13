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

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// replicationStatsReportID is the id of the row in the system. reports_meta
// table corresponding to the replication stats report (i.e. the
// system.replication_stats table).
const replicationStatsReportID = 3

// RangeReport represents the system.zone_range_status report.
type RangeReport map[ZoneKey]zoneRangeStatus

// zoneRangeStatus is the leaf of the RangeReport.
type zoneRangeStatus struct {
	numRanges       int32
	unavailable     int32
	underReplicated int32
	overReplicated  int32
}

// replicationStatsReportSaver manages the content and the saving of the report.
type replicationStatsReportSaver struct {
	stats               RangeReport
	previousVersion     RangeReport
	lastGenerated       time.Time
	lastUpdatedRowCount int
}

// makeReplicationStatsReportSaver creates a new report saver.
func makeReplicationStatsReportSaver() replicationStatsReportSaver {
	return replicationStatsReportSaver{
		stats: RangeReport{},
	}
}

// resetReport resets the report to an empty state.
func (r *replicationStatsReportSaver) resetReport() {
	r.stats = RangeReport{}
}

// LastUpdatedRowCount is the count of the rows that were touched during the last save.
func (r *replicationStatsReportSaver) LastUpdatedRowCount() int {
	return r.lastUpdatedRowCount
}

// AddZoneRangeStatus adds a row to the report.
func (r *replicationStatsReportSaver) AddZoneRangeStatus(
	zKey ZoneKey, unavailable bool, underReplicated bool, overReplicated bool,
) {
	if _, ok := r.stats[zKey]; !ok {
		r.stats[zKey] = zoneRangeStatus{}
	}
	rStat := r.stats[zKey]
	rStat.numRanges++
	if unavailable {
		rStat.unavailable++
	}
	if underReplicated {
		rStat.underReplicated++
	}
	if overReplicated {
		rStat.overReplicated++
	}
	r.stats[zKey] = rStat
}

func (r *replicationStatsReportSaver) loadPreviousVersion(
	ctx context.Context, ex sqlutil.InternalExecutor, txn *client.Txn,
) error {
	// The data for the previous save needs to be loaded if:
	// - this is the first time that we call this method and lastUpdatedAt has never been set
	// - in case that the lastUpdatedAt is set but is different than the timestamp in reports_meta
	//   this indicates that some other worker wrote after we did the write.
	if !r.lastGenerated.IsZero() {
		// check to see if the last timestamp for the update matches the local one.
		row, err := ex.QueryRow(
			ctx,
			"get-previous-timestamp",
			txn,
			"select generated from system.reports_meta where id = $1",
			replicationStatsReportID,
		)
		if err != nil {
			return err
		}

		// if the row is nil then this is the first time we are running and the reload is needed.
		if row != nil {
			generated, ok := row[0].(*tree.DTimestamp)
			if !ok {
				return errors.Errorf("Expected to get time from system.reports_meta but got %+v", row)
			}
			if generated.Time == r.lastGenerated {
				// No need to reload.
				return nil
			}
		}
	}
	const prevViolations = "select zone_id, subzone_id, total_ranges, " +
		"unavailable_ranges, under_replicated_ranges, over_replicated_ranges " +
		"from system.replication_stats"
	rows, err := ex.Query(
		ctx, "get-previous-replication-stats", txn, prevViolations,
	)
	if err != nil {
		return err
	}

	r.previousVersion = make(RangeReport, len(rows))
	for _, row := range rows {
		key := ZoneKey{}
		key.ZoneID = (uint32)(*row[0].(*tree.DInt))
		key.SubzoneID = SubzoneID(*row[1].(*tree.DInt))
		r.previousVersion[key] = zoneRangeStatus{
			(int32)(*row[2].(*tree.DInt)),
			(int32)(*row[3].(*tree.DInt)),
			(int32)(*row[4].(*tree.DInt)),
			(int32)(*row[5].(*tree.DInt)),
		}
	}

	return nil
}

func (r *replicationStatsReportSaver) updatePreviousVersion() {
	r.previousVersion = r.stats
	r.stats = make(RangeReport, len(r.previousVersion))
}

func (r *replicationStatsReportSaver) updateTimestamp(
	ctx context.Context, ex sqlutil.InternalExecutor, txn *client.Txn, reportTS time.Time,
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

// Save the report.
//
// reportTS is the time that will be set in the updated_at column for every row.
func (r *replicationStatsReportSaver) Save(
	ctx context.Context, reportTS time.Time, db *client.DB, ex sqlutil.InternalExecutor,
) error {
	r.lastUpdatedRowCount = 0
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		err := r.loadPreviousVersion(ctx, ex, txn)
		if err != nil {
			return err
		}

		err = r.updateTimestamp(ctx, ex, txn, reportTS)
		if err != nil {
			return err
		}

		for key, status := range r.stats {
			if err := r.upsertStats(
				ctx, reportTS, txn, key, status, db, ex,
			); err != nil {
				return err
			}
		}

		for key := range r.previousVersion {
			if _, ok := r.stats[key]; !ok {
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
	r.updatePreviousVersion()

	return nil
}

// upsertStat upserts a row into system.replication_stats.
//
// existing is used to decide is this is a new data.
func (r *replicationStatsReportSaver) upsertStats(
	ctx context.Context,
	reportTS time.Time,
	txn *client.Txn,
	key ZoneKey,
	stats zoneRangeStatus,
	db *client.DB,
	ex sqlutil.InternalExecutor,
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

	report *replicationStatsReportSaver
}

var _ rangeVisitor = &replicationStatsVisitor{}

func makeReplicationStatsVisitor(
	ctx context.Context,
	cfg *config.SystemConfig,
	nodeChecker nodeChecker,
	saver *replicationStatsReportSaver,
) replicationStatsVisitor {
	v := replicationStatsVisitor{
		cfg:         cfg,
		nodeChecker: nodeChecker,
		report:      saver,
	}
	v.report.resetReport()
	return v
}

// reset is part of the rangeVisitor interface.
func (v *replicationStatsVisitor) reset(ctx context.Context) {
	v.report.resetReport()

	// Iterate through all the zone configs to create report entries for all the
	// zones that have constraints. Otherwise, just iterating through the ranges
	// wouldn't create entries for zones that don't apply to any ranges.
	maxObjectID, err := v.cfg.GetLargestObjectID(0 /* maxID - return the largest ID in the config */)
	if err != nil {
		log.Fatalf(ctx, "unexpected failure to compute max object id: %s", err)
	}
	for i := uint32(1); i <= maxObjectID; i++ {
		zone, err := getZoneByID(i, v.cfg)
		if err != nil {
			log.Fatalf(ctx, "unexpected failure to compute max object id: %s", err)
		}
		if zone == nil {
			continue
		}
		v.report.AddZoneRangeStatus(
			MakeZoneKey(i, NoSubzone),
			false, /* unavailable */
			false, /* underReplicated */
			false /* overReplicated */)
	}
}

// visit is part of the rangeVisitor interface.
func (v *replicationStatsVisitor) visit(ctx context.Context, r roachpb.RangeDescriptor) {
	// Get the zone
	var zKey ZoneKey
	var zConfig *config.ZoneConfig
	found, err := visitZones(ctx, r, v.cfg,
		func(_ context.Context, zone *config.ZoneConfig, key ZoneKey) bool {
			if zone.NumReplicas == nil || *zone.NumReplicas == 0 {
				return false
			}
			zKey = key
			zConfig = zone
			return true
		})
	if err != nil {
		log.Fatalf(ctx, "unexpected error visiting zones: %s", err)
	}
	if !found {
		log.Errorf(ctx, "no zone config with replication attributes found for range: %s", &r)
		return
	}

	underReplicated := *zConfig.NumReplicas > int32(len(r.Replicas().Voters()))
	overReplicated := *zConfig.NumReplicas < int32(len(r.Replicas().Voters()))
	var liveNodeCount int
	for _, rep := range r.Replicas().Voters() {
		if v.nodeChecker(rep.NodeID) {
			liveNodeCount++
		}
	}
	unavailable := liveNodeCount < (len(r.Replicas().Voters())/2 + 1)

	v.report.AddZoneRangeStatus(zKey, unavailable, underReplicated, overReplicated)
}
