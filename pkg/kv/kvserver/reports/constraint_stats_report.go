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
	"fmt"
	"strings"
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

// replicationConstraintsReportID is the id of the row in the system.
// reports_meta table corresponding to the constraints conformance report (i.e.
// the system.replicationConstraintsReportID table).
const replicationConstraintsReportID reportID = 1

// ConstraintReport contains information about the constraint conformance for
// the cluster's data.
type ConstraintReport map[ConstraintStatusKey]ConstraintStatus

// replicationConstraintStatsReportSaver deals with saving a ConstrainReport to
// the database. The idea is for it to be used to save new version of the report
// over and over. It maintains the previously-saved version of the report in
// order to speed-up the saving of the next one.
type replicationConstraintStatsReportSaver struct {
	previousVersion     ConstraintReport
	lastGenerated       time.Time
	lastUpdatedRowCount int
}

// makeReplicationConstraintStatusReportSaver creates a new report saver.
func makeReplicationConstraintStatusReportSaver() replicationConstraintStatsReportSaver {
	return replicationConstraintStatsReportSaver{}
}

// LastUpdatedRowCount is the count of the rows that were touched during the last save.
func (r *replicationConstraintStatsReportSaver) LastUpdatedRowCount() int {
	return r.lastUpdatedRowCount
}

// ConstraintStatus is the leaf in the constraintReport.
type ConstraintStatus struct {
	FailRangeCount int
}

// ConstraintType indicates what type of constraint is an entry in the
// constraint conformance report talking about.
type ConstraintType string

const (
	// Constraint means that the entry refers to a constraint (i.e. a member of
	// the constraints field in a zone config).
	Constraint ConstraintType = "constraint"
	// TODO(andrei): add leaseholder preference
)

// Less compares two ConstraintTypes.
func (t ConstraintType) Less(other ConstraintType) bool {
	return -1 == strings.Compare(string(t), string(other))
}

// ConstraintRepr is a string representation of a constraint.
type ConstraintRepr string

// Less compares two ConstraintReprs.
func (c ConstraintRepr) Less(other ConstraintRepr) bool {
	return -1 == strings.Compare(string(c), string(other))
}

// ConstraintStatusKey represents the key in the ConstraintReport.
type ConstraintStatusKey struct {
	ZoneKey
	ViolationType ConstraintType
	Constraint    ConstraintRepr
}

func (k ConstraintStatusKey) String() string {
	return fmt.Sprintf("zone:%s type:%s constraint:%s", k.ZoneKey, k.ViolationType, k.Constraint)
}

// Less compares two ConstraintStatusKeys.
func (k ConstraintStatusKey) Less(other ConstraintStatusKey) bool {
	if k.ZoneKey.Less(other.ZoneKey) {
		return true
	}
	if other.ZoneKey.Less(k.ZoneKey) {
		return false
	}
	if k.ViolationType.Less(other.ViolationType) {
		return true
	}
	if other.ViolationType.Less(k.ViolationType) {
		return true
	}
	return k.Constraint.Less(other.Constraint)
}

// AddViolation add a constraint that is being violated for a given range. Each call
// will increase the number of ranges that failed.
func (r ConstraintReport) AddViolation(z ZoneKey, t ConstraintType, c ConstraintRepr) {
	k := ConstraintStatusKey{
		ZoneKey:       z,
		ViolationType: t,
		Constraint:    c,
	}
	if _, ok := r[k]; !ok {
		r[k] = ConstraintStatus{}
	}
	cRep := r[k]
	cRep.FailRangeCount++
	r[k] = cRep
}

// ensureEntry us used to add an entry to the report even if there is no violation.
func (r ConstraintReport) ensureEntry(z ZoneKey, t ConstraintType, c ConstraintRepr) {
	k := ConstraintStatusKey{
		ZoneKey:       z,
		ViolationType: t,
		Constraint:    c,
	}
	if _, ok := r[k]; !ok {
		r[k] = ConstraintStatus{}
	}
}

func (r ConstraintReport) ensureEntries(key ZoneKey, zone *zonepb.ZoneConfig) {
	for _, conjunction := range zone.Constraints {
		r.ensureEntry(key, Constraint, ConstraintRepr(conjunction.String()))
	}
	for i, sz := range zone.Subzones {
		szKey := ZoneKey{ZoneID: key.ZoneID, SubzoneID: base.SubzoneIDFromIndex(i)}
		r.ensureEntries(szKey, &sz.Config)
	}
}

func (r *replicationConstraintStatsReportSaver) loadPreviousVersion(
	ctx context.Context, ex sqlutil.InternalExecutor, txn *kv.Txn,
) error {
	// The data for the previous save needs to be loaded if:
	// - this is the first time that we call this method and lastUpdatedAt has never been set
	// - in case that the lastUpdatedAt is set but is different than the timestamp in reports_meta
	//   this indicates that some other worker wrote after we did the write.
	if !r.lastGenerated.IsZero() {
		generated, err := getReportGenerationTime(ctx, replicationConstraintsReportID, ex, txn)
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
	const prevViolations = "select zone_id, subzone_id, type, config, " +
		"violating_ranges from system.replication_constraint_stats"
	it, err := ex.QueryIterator(
		ctx, "get-previous-replication-constraint-stats", txn, prevViolations,
	)
	if err != nil {
		return err
	}

	r.previousVersion = make(ConstraintReport)
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		key := ConstraintStatusKey{}
		key.ZoneID = (config.SystemTenantObjectID)(*row[0].(*tree.DInt))
		key.SubzoneID = base.SubzoneID((*row[1].(*tree.DInt)))
		key.ViolationType = (ConstraintType)(*row[2].(*tree.DString))
		key.Constraint = (ConstraintRepr)(*row[3].(*tree.DString))
		r.previousVersion[key] = ConstraintStatus{(int)(*row[4].(*tree.DInt))}
	}
	return err
}

func (r *replicationConstraintStatsReportSaver) updateTimestamp(
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
		"timestamp-upsert-replication-constraint-stats",
		txn,
		"upsert into system.reports_meta(id, generated) values($1, $2)",
		replicationConstraintsReportID,
		reportTS,
	)
	return err
}

// Save the report in the database.
//
// report should not be used by the caller any more after this call; the callee
// takes ownership.
// reportTS is the time that will be set in the updated_at column for every row.
func (r *replicationConstraintStatsReportSaver) Save(
	ctx context.Context,
	report ConstraintReport,
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

		for k, zoneCons := range report {
			if err := r.upsertConstraintStatus(
				ctx, reportTS, txn, k, zoneCons.FailRangeCount, db, ex,
			); err != nil {
				return err
			}
		}

		for key := range r.previousVersion {
			if _, ok := report[key]; !ok {
				_, err := ex.Exec(
					ctx,
					"delete-old-replication-constraint-stats",
					txn,
					"delete from system.replication_constraint_stats "+
						"where zone_id = $1 and subzone_id = $2 and type = $3 and config = $4",
					key.ZoneID,
					key.SubzoneID,
					key.ViolationType,
					key.Constraint,
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

// upsertConstraintStatus upserts a row into system.replication_constraint_stats.
//
// existing is used to decide is this is a new violation.
func (r *replicationConstraintStatsReportSaver) upsertConstraintStatus(
	ctx context.Context,
	reportTS time.Time,
	txn *kv.Txn,
	key ConstraintStatusKey,
	violationCount int,
	db *kv.DB,
	ex sqlutil.InternalExecutor,
) error {
	var err error
	previousStatus, hasOldVersion := r.previousVersion[key]
	if hasOldVersion && previousStatus.FailRangeCount == violationCount {
		// No change in the status so no update.
		return nil
	} else if violationCount != 0 {
		if previousStatus.FailRangeCount != 0 {
			// Updating an old violation. No need to update the start timestamp.
			_, err = ex.Exec(
				ctx, "upsert-replication-constraint-stat", txn,
				"upsert into system.replication_constraint_stats(report_id, zone_id, subzone_id, type, "+
					"config, violating_ranges) values($1, $2, $3, $4, $5, $6)",
				replicationConstraintsReportID,
				key.ZoneID, key.SubzoneID, key.ViolationType, key.Constraint, violationCount,
			)
		} else if previousStatus.FailRangeCount == 0 {
			// New violation detected. Need to update the start timestamp.
			_, err = ex.Exec(
				ctx, "upsert-replication-constraint-stat", txn,
				"upsert into system.replication_constraint_stats(report_id, zone_id, subzone_id, type, "+
					"config, violating_ranges, violation_start) values($1, $2, $3, $4, $5, $6, $7)",
				replicationConstraintsReportID,
				key.ZoneID, key.SubzoneID, key.ViolationType, key.Constraint, violationCount, reportTS,
			)
		}
	} else {
		// Need to set the violation start to null as there was an violation that doesn't exist anymore.
		_, err = ex.Exec(
			ctx, "upsert-replication-constraint-stat", txn,
			"upsert into system.replication_constraint_stats(report_id, zone_id, subzone_id, type, config, "+
				"violating_ranges, violation_start) values($1, $2, $3, $4, $5, $6, null)",
			replicationConstraintsReportID,
			key.ZoneID, key.SubzoneID, key.ViolationType, key.Constraint, violationCount,
		)
	}

	if err != nil {
		return err
	}

	r.lastUpdatedRowCount++
	return nil
}

// constraintConformanceVisitor is a visitor that, when passed to visitRanges(),
// computes the constraint conformance report (i.e. the
// system.replication_constraint_stats table).
type constraintConformanceVisitor struct {
	cfg           *config.SystemConfig
	storeResolver StoreResolver

	// report is the output of the visitor. visit*() methods populate it.
	// After visiting all the ranges, it can be retrieved with Report().
	report   ConstraintReport
	visitErr bool

	// prevZoneKey and prevConstraints maintain state from one range to the next.
	// This state can be reused when a range is covered by the same zone config as
	// the previous one. Reusing it speeds up the report generation.
	prevZoneKey     ZoneKey
	prevConstraints []zonepb.ConstraintsConjunction
}

var _ rangeVisitor = &constraintConformanceVisitor{}

func makeConstraintConformanceVisitor(
	ctx context.Context, cfg *config.SystemConfig, storeResolver StoreResolver,
) constraintConformanceVisitor {
	v := constraintConformanceVisitor{
		cfg:           cfg,
		storeResolver: storeResolver,
	}
	v.reset(ctx)
	return v
}

// failed is part of the rangeVisitor interface.
func (v *constraintConformanceVisitor) failed() bool {
	return v.visitErr
}

// Report returns the ConstraintReport that was populated by previous visit*()
// calls.
func (v *constraintConformanceVisitor) Report() ConstraintReport {
	return v.report
}

// reset is part of the rangeVisitor interface.
func (v *constraintConformanceVisitor) reset(ctx context.Context) {
	*v = constraintConformanceVisitor{
		cfg:           v.cfg,
		storeResolver: v.storeResolver,
		report:        make(ConstraintReport, len(v.report)),
	}

	// Iterate through all the zone configs to create report entries for all the
	// zones that have constraints. Otherwise, just iterating through the ranges
	// wouldn't create entries for constraints that aren't violated, and
	// definitely not for zones that don't apply to any ranges.
	maxObjectID, err := v.cfg.GetLargestObjectID(
		0 /* maxID - return the largest ID in the config */, keys.PseudoTableIDs)
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
		v.report.ensureEntries(MakeZoneKey(i, NoSubzone), zone)
	}
}

// visitNewZone is part of the rangeVisitor interface.
func (v *constraintConformanceVisitor) visitNewZone(
	ctx context.Context, r *roachpb.RangeDescriptor,
) (retErr error) {

	defer func() {
		v.visitErr = retErr != nil
	}()

	// Find the applicable constraints, which may be inherited.
	var constraints []zonepb.ConstraintsConjunction
	var zKey ZoneKey
	_, err := visitZones(ctx, r, v.cfg, ignoreSubzonePlaceholders,
		func(_ context.Context, zone *zonepb.ZoneConfig, key ZoneKey) bool {
			if zone.Constraints == nil {
				return false
			}
			constraints = zone.Constraints
			zKey = key
			return true
		})
	if err != nil {
		return errors.Errorf("unexpected error visiting zones: %s", err)
	}
	v.prevZoneKey = zKey
	v.prevConstraints = constraints
	v.countRange(ctx, r, zKey, constraints)
	return nil
}

// visitSameZone is part of the rangeVisitor interface.
func (v *constraintConformanceVisitor) visitSameZone(
	ctx context.Context, r *roachpb.RangeDescriptor,
) {
	v.countRange(ctx, r, v.prevZoneKey, v.prevConstraints)
}

func (v *constraintConformanceVisitor) countRange(
	ctx context.Context,
	r *roachpb.RangeDescriptor,
	key ZoneKey,
	constraints []zonepb.ConstraintsConjunction,
) {
	storeDescs := v.storeResolver(r)
	violated := getViolations(ctx, storeDescs, constraints)
	for _, c := range violated {
		v.report.AddViolation(key, Constraint, c)
	}
}

// getViolations returns the list of constraints violated by a range. The range
// is represented by the descriptors of the replicas' stores.
func getViolations(
	ctx context.Context,
	storeDescs []roachpb.StoreDescriptor,
	constraintConjunctions []zonepb.ConstraintsConjunction,
) []ConstraintRepr {
	var res []ConstraintRepr
	// Evaluate all zone constraints for the stores (i.e. replicas) of the given range.
	for _, conjunction := range constraintConjunctions {
		replicasRequiredToMatch := int(conjunction.NumReplicas)
		if replicasRequiredToMatch == 0 {
			replicasRequiredToMatch = len(storeDescs)
		}
		for _, c := range conjunction.Constraints {
			if !constraintSatisfied(c, replicasRequiredToMatch, storeDescs) {
				res = append(res, ConstraintRepr(conjunction.String()))
				break
			}
		}
	}
	return res
}

// constraintSatisfied checks that a range (represented by its replicas' stores)
// satisfies a constraint.
func constraintSatisfied(
	c zonepb.Constraint, replicasRequiredToMatch int, storeDescs []roachpb.StoreDescriptor,
) bool {
	passCount := 0
	for _, storeDesc := range storeDescs {
		// Consider stores for which we have no information to pass everything.
		if storeDesc.StoreID == 0 {
			passCount++
			continue
		}
		if zonepb.StoreSatisfiesConstraint(storeDesc, c) {
			passCount++
		}
	}
	return replicasRequiredToMatch <= passCount
}
