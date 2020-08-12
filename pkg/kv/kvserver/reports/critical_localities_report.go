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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

// criticalLocalitiesReportID is the id of the row in the system. reports_meta
// table corresponding to the critical localities report (i.e. the
// system.replication_critical_localities table).
const criticalLocalitiesReportID reportID = 2

type localityKey struct {
	ZoneKey
	locality LocalityRepr
}

// LocalityRepr is a representation of a locality.
type LocalityRepr string

type localityStatus struct {
	atRiskRanges int32
}

// LocalityReport stores the range status information for each locality and
// applicable zone.
type LocalityReport map[localityKey]localityStatus

// replicationCriticalLocalitiesReportSaver deals with saving a LocalityReport
// to the database. The idea is for it to be used to save new version of the
// report over and over. It maintains the previously-saved version of the report
// in order to speed-up the saving of the next one.
type replicationCriticalLocalitiesReportSaver struct {
	previousVersion     LocalityReport
	lastGenerated       time.Time
	lastUpdatedRowCount int
}

// makeReplicationCriticalLocalitiesReportSaver creates a new report saver.
func makeReplicationCriticalLocalitiesReportSaver() replicationCriticalLocalitiesReportSaver {
	return replicationCriticalLocalitiesReportSaver{}
}

// LastUpdatedRowCount is the count of the rows that were touched during the last save.
func (r *replicationCriticalLocalitiesReportSaver) LastUpdatedRowCount() int {
	return r.lastUpdatedRowCount
}

// CountRangeAtRisk increments the number of ranges at-risk for the report entry
// corresponding to the given zone and locality. In other words, the report will
// count the respective locality as critical for one more range in the given
// zone.
func (r LocalityReport) CountRangeAtRisk(zKey ZoneKey, loc LocalityRepr) {
	lKey := localityKey{
		ZoneKey:  zKey,
		locality: loc,
	}
	if _, ok := r[lKey]; !ok {
		r[lKey] = localityStatus{}
	}
	lStat := r[lKey]
	lStat.atRiskRanges++
	r[lKey] = lStat
}

func (r *replicationCriticalLocalitiesReportSaver) loadPreviousVersion(
	ctx context.Context, ex sqlutil.InternalExecutor, txn *kv.Txn,
) error {
	// The data for the previous save needs to be loaded if:
	// - this is the first time that we call this method and lastUpdatedAt has never been set
	// - in case that the lastUpdatedAt is set but is different than the timestamp in reports_meta
	//   this indicates that some other worker wrote after we did the write.
	if !r.lastGenerated.IsZero() {
		generated, err := getReportGenerationTime(ctx, criticalLocalitiesReportID, ex, txn)
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
	const prevViolations = "select zone_id, subzone_id, locality, at_risk_ranges " +
		"from system.replication_critical_localities"
	rows, err := ex.Query(
		ctx, "get-previous-replication-critical-localities", txn, prevViolations,
	)
	if err != nil {
		return err
	}

	r.previousVersion = make(LocalityReport, len(rows))
	for _, row := range rows {
		key := localityKey{}
		key.ZoneID = (config.SystemTenantObjectID)(*row[0].(*tree.DInt))
		key.SubzoneID = base.SubzoneID(*row[1].(*tree.DInt))
		key.locality = (LocalityRepr)(*row[2].(*tree.DString))
		r.previousVersion[key] = localityStatus{(int32)(*row[3].(*tree.DInt))}
	}

	return nil
}

func (r *replicationCriticalLocalitiesReportSaver) updateTimestamp(
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
		"timestamp-upsert-replication-critical-localities",
		txn,
		"upsert into system.reports_meta(id, generated) values($1, $2)",
		criticalLocalitiesReportID,
		reportTS,
	)
	return err
}

// Save the report to the database.
//
// report should not be used by the caller any more after this call; the callee
// takes ownership.
// reportTS is the time that will be set in the updated_at column for every row.
func (r *replicationCriticalLocalitiesReportSaver) Save(
	ctx context.Context,
	report LocalityReport,
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
			if err := r.upsertLocality(
				ctx, reportTS, txn, key, status, db, ex,
			); err != nil {
				return err
			}
		}

		for key := range r.previousVersion {
			if _, ok := report[key]; !ok {
				_, err := ex.Exec(
					ctx,
					"delete-old-replication-critical-localities",
					txn,
					"delete from system.replication_critical_localities "+
						"where zone_id = $1 and subzone_id = $2 and locality = $3",
					key.ZoneID,
					key.SubzoneID,
					key.locality,
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

// upsertLocality upserts a row into system.replication_critical_localities.
//
// existing is used to decide is this is a new violation.
func (r *replicationCriticalLocalitiesReportSaver) upsertLocality(
	ctx context.Context,
	reportTS time.Time,
	txn *kv.Txn,
	key localityKey,
	status localityStatus,
	db *kv.DB,
	ex sqlutil.InternalExecutor,
) error {
	var err error
	previousStatus, hasOldVersion := r.previousVersion[key]
	if hasOldVersion && previousStatus.atRiskRanges == status.atRiskRanges {
		// No change in the status so no update.
		return nil
	}

	// Updating an old row.
	_, err = ex.Exec(
		ctx, "upsert-replication-critical-localities", txn,
		"upsert into system.replication_critical_localities(report_id, zone_id, subzone_id, "+
			"locality, at_risk_ranges) values($1, $2, $3, $4, $5)",
		criticalLocalitiesReportID,
		key.ZoneID, key.SubzoneID, key.locality, status.atRiskRanges,
	)

	if err != nil {
		return err
	}

	r.lastUpdatedRowCount++
	return nil
}

// criticalLocalitiesVisitor is a visitor that, when passed to visitRanges(), builds
// a LocalityReport.
type criticalLocalitiesVisitor struct {
	allLocalities map[roachpb.NodeID]map[string]roachpb.Locality
	cfg           *config.SystemConfig
	storeResolver StoreResolver
	nodeChecker   nodeChecker

	// report is the output of the visitor. visit*() methods populate it.
	// After visiting all the ranges, it can be retrieved with Report().
	report   LocalityReport
	visitErr bool

	// prevZoneKey maintains state from one range to the next. This state can be
	// reused when a range is covered by the same zone config as the previous one.
	// Reusing it speeds up the report generation.
	prevZoneKey ZoneKey
}

var _ rangeVisitor = &criticalLocalitiesVisitor{}

func makeCriticalLocalitiesVisitor(
	ctx context.Context,
	nodeLocalities map[roachpb.NodeID]roachpb.Locality,
	cfg *config.SystemConfig,
	storeResolver StoreResolver,
	nodeChecker nodeChecker,
) criticalLocalitiesVisitor {
	allLocalities := expandLocalities(nodeLocalities)
	v := criticalLocalitiesVisitor{
		allLocalities: allLocalities,
		cfg:           cfg,
		storeResolver: storeResolver,
		nodeChecker:   nodeChecker,
	}
	v.reset(ctx)
	return v
}

// expandLocalities expands each locality in its input into multiple localities,
// each at a different level of granularity. For example the locality
// "region=r1,dc=dc1,az=az1" is expanded into ["region=r1", "region=r1,dc=dc1",
// "region=r1,dc=dc1,az=az1"].
// The localities are returned in a format convenient for the
// criticalLocalitiesVisitor.
func expandLocalities(
	nodeLocalities map[roachpb.NodeID]roachpb.Locality,
) map[roachpb.NodeID]map[string]roachpb.Locality {
	res := make(map[roachpb.NodeID]map[string]roachpb.Locality)
	for nid, loc := range nodeLocalities {
		if len(loc.Tiers) == 0 {
			res[nid] = nil
			continue
		}
		res[nid] = make(map[string]roachpb.Locality, len(loc.Tiers))
		for i := range loc.Tiers {
			partialLoc := roachpb.Locality{Tiers: make([]roachpb.Tier, i+1)}
			copy(partialLoc.Tiers, loc.Tiers[:i+1])
			res[nid][partialLoc.String()] = partialLoc
		}
	}
	return res
}

// failed is part of the rangeVisitor interface.
func (v *criticalLocalitiesVisitor) failed() bool {
	return v.visitErr
}

// Report returns the LocalityReport that was populated by previous visit*()
// calls.
func (v *criticalLocalitiesVisitor) Report() LocalityReport {
	return v.report
}

// reset is part of the rangeVisitor interface.
func (v *criticalLocalitiesVisitor) reset(ctx context.Context) {
	*v = criticalLocalitiesVisitor{
		allLocalities: v.allLocalities,
		cfg:           v.cfg,
		storeResolver: v.storeResolver,
		nodeChecker:   v.nodeChecker,
		report:        make(LocalityReport, len(v.report)),
	}
}

// visitNewZone is part of the rangeVisitor interface.
func (v *criticalLocalitiesVisitor) visitNewZone(
	ctx context.Context, r *roachpb.RangeDescriptor,
) (retErr error) {

	defer func() {
		v.visitErr = retErr != nil
	}()

	// Get the zone.
	var zKey ZoneKey
	found, err := visitZones(ctx, r, v.cfg, ignoreSubzonePlaceholders,
		func(_ context.Context, zone *zonepb.ZoneConfig, key ZoneKey) bool {
			if !zoneChangesReplication(zone) {
				return false
			}
			zKey = key
			return true
		})
	if err != nil {
		return errors.AssertionFailedf("unexpected error visiting zones: %s", err)
	}
	if !found {
		return errors.AssertionFailedf("no suitable zone config found for range: %s", r)
	}
	v.prevZoneKey = zKey

	v.countRange(ctx, zKey, r)
	return nil
}

// visitSameZone is part of the rangeVisitor interface.
func (v *criticalLocalitiesVisitor) visitSameZone(ctx context.Context, r *roachpb.RangeDescriptor) {
	v.countRange(ctx, v.prevZoneKey, r)
}

func (v *criticalLocalitiesVisitor) countRange(
	ctx context.Context, zoneKey ZoneKey, r *roachpb.RangeDescriptor,
) {
	stores := v.storeResolver(r)

	// Collect all the localities of all the replicas. Note that we collect
	// "expanded" localities: if a replica has a multi-tier locality like
	// "region:us-east,dc=new-york", we collect both "region:us-east" and
	// "region:us-east,dc=new-york".
	dedupLocal := make(map[string]roachpb.Locality)
	for _, rep := range r.Replicas().All() {
		for s, loc := range v.allLocalities[rep.NodeID] {
			if _, ok := dedupLocal[s]; ok {
				continue
			}
			dedupLocal[s] = loc
		}
	}

	// Any of the localities of any of the nodes could be critical. We'll check
	// them one by one.
	for _, loc := range dedupLocal {
		processLocalityForRange(ctx, r, zoneKey, v.report, loc, v.nodeChecker, stores)
	}
}

// processLocalityForRange checks a single locality constraint against a
// range with replicas in each of the stores given, contributing to rep.
func processLocalityForRange(
	ctx context.Context,
	r *roachpb.RangeDescriptor,
	zoneKey ZoneKey,
	rep LocalityReport,
	loc roachpb.Locality,
	nodeChecker nodeChecker,
	storeDescs []roachpb.StoreDescriptor,
) {
	// Compute the required quorum and the number of live nodes. If the number of
	// live nodes gets lower than the required quorum then the range is already
	// unavailable.
	quorumCount := len(r.Replicas().Voters())/2 + 1
	liveNodeCount := len(storeDescs)
	for _, storeDesc := range storeDescs {
		isStoreLive := nodeChecker(storeDesc.Node.NodeID)
		if !isStoreLive {
			if liveNodeCount >= quorumCount {
				liveNodeCount--
				if liveNodeCount < quorumCount {
					break
				}
			}
		}
	}

	localityToConstraints := func(loc roachpb.Locality) zonepb.ConstraintsConjunction {
		c := zonepb.ConstraintsConjunction{
			Constraints: make([]zonepb.Constraint, 0, len(loc.Tiers)),
		}
		for _, tier := range loc.Tiers {
			c.Constraints = append(c.Constraints, zonepb.Constraint{
				Type: zonepb.Constraint_REQUIRED, Key: tier.Key, Value: tier.Value,
			})
		}
		return c
	}

	locStr := LocalityRepr(loc.String())
	c := localityToConstraints(loc)
	passCount := 0
	for _, storeDesc := range storeDescs {
		storeHasConstraint := true
		for _, constraint := range c.Constraints {
			// For required constraints - consider unavailable nodes as not matching.
			if !zonepb.StoreMatchesConstraint(storeDesc, constraint) {
				storeHasConstraint = false
				break
			}
		}

		if storeHasConstraint && nodeChecker(storeDesc.Node.NodeID) {
			passCount++
		}
	}

	// If the live nodes outside of the given locality are not enough to
	// form quorum then this locality is critical.
	if quorumCount > liveNodeCount-passCount {
		rep.CountRangeAtRisk(zoneKey, locStr)
	}
}
