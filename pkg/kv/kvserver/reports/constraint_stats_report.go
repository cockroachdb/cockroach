// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	// VoterConstraint means that the entry refers to a voter_constraint (i.e. a
	// member of voter_constraint field in a zone config).
	VoterConstraint ConstraintType = "voter_constraint"
	// TODO(andrei): add leaseholder preference
)

// Less compares two ConstraintTypes.
func (t ConstraintType) Less(other ConstraintType) bool {
	return strings.Compare(string(t), string(other)) == -1
}

// ConstraintRepr is a string representation of a constraint.
type ConstraintRepr string

// Less compares two ConstraintReprs.
func (c ConstraintRepr) Less(other ConstraintRepr) bool {
	return strings.Compare(string(c), string(other)) == -1
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
		return false
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
	for _, conjunction := range zone.VoterConstraints {
		r.ensureEntry(key, VoterConstraint, ConstraintRepr(conjunction.String()))
	}
	for i, sz := range zone.Subzones {
		szKey := ZoneKey{ZoneID: key.ZoneID, SubzoneID: base.SubzoneIDFromIndex(i)}
		r.ensureEntries(szKey, &sz.Config)
	}
}

func (r *replicationConstraintStatsReportSaver) loadPreviousVersion(
	ctx context.Context, ex isql.Executor, txn *kv.Txn,
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
		key.ZoneID = (config.ObjectID)(*row[0].(*tree.DInt))
		key.SubzoneID = base.SubzoneID((*row[1].(*tree.DInt)))
		key.ViolationType = (ConstraintType)(*row[2].(*tree.DString))
		key.Constraint = (ConstraintRepr)(*row[3].(*tree.DString))
		r.previousVersion[key] = ConstraintStatus{(int)(*row[4].(*tree.DInt))}
	}
	return err
}

func (r *replicationConstraintStatsReportSaver) updateTimestamp(
	ctx context.Context, ex isql.Executor, txn *kv.Txn, reportTS time.Time,
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
	ctx context.Context, report ConstraintReport, reportTS time.Time, db *kv.DB, ex isql.Executor,
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
	ex isql.Executor,
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

type replicaPredicate func(r roachpb.ReplicaDescriptor) bool

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

	// Zone checker maintain a zone config state internally and can be reused when
	// a range is covered by the same zone config as the previous one. Reusing it
	// speeds up the report generation.
	// It is recreated every time a range is processed with a different zone key.
	zoneChecker constraintConformanceChecker
}

var _ rangeVisitor = &constraintConformanceVisitor{}

func makeConstraintConformanceVisitor(
	ctx context.Context, cfg *config.SystemConfig, storeResolver StoreResolver,
) constraintConformanceVisitor {
	v := constraintConformanceVisitor{
		cfg:           cfg,
		storeResolver: storeResolver,
		report:        make(ConstraintReport),
	}
	v.init(ctx)
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

func (v *constraintConformanceVisitor) init(ctx context.Context) {
	// Iterate through all the zone configs to create report entries for all the
	// zones that have constraints. Otherwise, just iterating through the ranges
	// wouldn't create entries for constraints that aren't violated, and
	// definitely not for zones that don't apply to any ranges.
	maxObjectID, err := v.cfg.GetLargestObjectID(
		0 /* maxReservedDescID - return the largest ID in the config */, keys.PseudoTableIDs)
	if err != nil {
		log.Fatalf(ctx, "unexpected failure to compute max object id: %s", err)
	}
	for i := config.ObjectID(1); i <= maxObjectID; i++ {
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

// constraintCheckPolicy defines a set of predicates that define subsets of
// replicas that are checked against conjunctions.
// In order for constraint type to satisfy, each subset of replicas needs to
// satisfy provided constraint conjunction.
// For example voter constraint should match both subset of replicas
// representing outgoing and incoming consensus.
type constraintCheckPolicy struct {
	predicates map[zonepb.Constraint_Type][]replicaPredicate
}

// getViolations finds constraint violations according to sets of predicates
// defined in policy.
func (p constraintCheckPolicy) getViolations(
	r *roachpb.RangeDescriptor,
	storeResolver StoreResolver,
	conjunctions []zonepb.ConstraintsConjunction,
) (res []ConstraintRepr) {

	checkConstraints := func(cj zonepb.ConstraintsConjunction) (bool, ConstraintRepr) {
		for _, cc := range cj.Constraints {
			t := cc.Type
			if t == zonepb.Constraint_DEPRECATED_POSITIVE {
				t = zonepb.Constraint_REQUIRED
			}

			// Check all store variants defined by policy.
			for _, p := range p.predicates[t] {
				rds := r.Replicas().FilterToDescriptors(p)
				storeDescs := make([]roachpb.StoreDescriptor, len(rds))
				for i, r := range rds {
					storeDescs[i] = storeResolver(r.StoreID)
				}
				// Run each constraint against all store variants to find violations.
				replicasRequiredToMatch := int(cj.NumReplicas)
				if replicasRequiredToMatch == 0 {
					replicasRequiredToMatch = len(storeDescs)
				}
				if !constraintSatisfied(cc, replicasRequiredToMatch, storeDescs) {
					return true, ConstraintRepr(cj.String())
				}
			}
		}
		return false, ""
	}

	for _, cj := range conjunctions {
		if ok, repr := checkConstraints(cj); ok {
			res = append(res, repr)
		}
	}
	return res
}

// visitNewZone is part of the rangeVisitor interface.
func (v *constraintConformanceVisitor) visitNewZone(
	ctx context.Context, r *roachpb.RangeDescriptor,
) (retErr error) {

	defer func() {
		v.visitErr = retErr != nil
	}()

	// Find the applicable constraints, which may be inherited.
	var numVoters int
	var constraints []zonepb.ConstraintsConjunction
	var voterConstraints []zonepb.ConstraintsConjunction
	var zKey ZoneKey
	_, err := visitZones(ctx, r, v.cfg, ignoreSubzonePlaceholders,
		func(_ context.Context, zone *zonepb.ZoneConfig, key ZoneKey) bool {
			if zone.Constraints == nil {
				return false
			}
			// Check num voters and only set it if it is different from num replicas.
			var numReplicas int32
			if zone.NumReplicas != nil {
				numReplicas = *zone.NumReplicas
			}
			if zone.NumVoters != nil && numReplicas != *zone.NumVoters {
				numVoters = int(*zone.NumVoters)
			}
			constraints = zone.Constraints
			voterConstraints = zone.VoterConstraints
			zKey = key
			return true
		})
	if err != nil {
		return errors.Wrap(err, "unexpected error visiting zones")
	}
	v.zoneChecker = constraintConformanceChecker{
		zoneKey:          zKey,
		numVoters:        numVoters,
		constraints:      constraints,
		voterConstraints: voterConstraints,
		storeResolver:    v.storeResolver,
		report:           &v.report,
	}
	v.zoneChecker.checkZone(ctx, r)
	return nil
}

func (v *constraintConformanceVisitor) visitSameZone(
	ctx context.Context, r *roachpb.RangeDescriptor,
) {
	v.zoneChecker.checkZone(ctx, r)
}

type constraintConformanceChecker struct {
	zoneKey          ZoneKey
	numVoters        int
	constraints      []zonepb.ConstraintsConjunction
	voterConstraints []zonepb.ConstraintsConjunction

	storeResolver StoreResolver
	report        *ConstraintReport
}

// visitSameZone is part of the rangeVisitor interface.
func (v *constraintConformanceChecker) checkZone(ctx context.Context, r *roachpb.RangeDescriptor) {
	// replicaConstraintsAllVoters are applied to replica constraints when number
	// of voters are not specified e.g. equal to number of replicas implicitly or
	// equal to number of voters explicitly.
	var replicaConstraintsAllVoters = constraintCheckPolicy{
		predicates: map[zonepb.Constraint_Type][]replicaPredicate{
			zonepb.Constraint_REQUIRED: {
				isInIncomingQuorumOrNonVoter, isInOutgoingQuorumOrNonVoter,
			},
			zonepb.Constraint_PROHIBITED: {
				isAny,
			},
		},
	}

	// replicaConstraintsWithNonVoters are applied to replica constraints when
	// number of voters are explicitly specified and is below total number of
	// replicas.
	var replicaConstraintsWithNonVoters = constraintCheckPolicy{
		predicates: map[zonepb.Constraint_Type][]replicaPredicate{
			// Note that required predicate are replicas that we can route reads to
			// hence LEARNER and VOTER_DEMOTING and VOTER_OUTGOING are excluded. Even
			// if DEMOTING/OUTGOING replicas can still serve reads we don't route to
			// them and they should disappear momentarily, so we keep constraint
			// checks in sync with routing logic in DistSender.
			zonepb.Constraint_REQUIRED: {
				isInIncomingQuorumOrNonVoter,
			},
			zonepb.Constraint_PROHIBITED: {
				isAny,
			},
		},
	}

	// voterConstraints are applied when voter constraints are explicitly specified
	// in zone config to voter constraints.
	var voterConstraints = constraintCheckPolicy{
		predicates: map[zonepb.Constraint_Type][]replicaPredicate{
			zonepb.Constraint_REQUIRED: {
				isInIncomingQuorum, isInOutgoingQuorum,
			},
			zonepb.Constraint_PROHIBITED: {
				isVoter,
			},
		},
	}

	if v.numVoters != 0 {
		v.countRange(ctx, r, v.zoneKey, Constraint, replicaConstraintsWithNonVoters,
			v.constraints)
	} else {
		v.countRange(ctx, r, v.zoneKey, Constraint, replicaConstraintsAllVoters, v.constraints)
	}
	v.countRange(ctx, r, v.zoneKey, VoterConstraint, voterConstraints, v.voterConstraints)
}

func (v *constraintConformanceChecker) countRange(
	ctx context.Context,
	r *roachpb.RangeDescriptor,
	key ZoneKey,
	t ConstraintType,
	policy constraintCheckPolicy,
	constraints []zonepb.ConstraintsConjunction,
) {
	for _, violation := range policy.getViolations(r, v.storeResolver, constraints) {
		v.report.AddViolation(key, t, violation)
	}
}

func isAny(_ roachpb.ReplicaDescriptor) bool {
	return true
}

func isInIncomingQuorumOrNonVoter(r roachpb.ReplicaDescriptor) bool {
	return isInIncomingQuorum(r) || isNonVoter(r)
}

func isInOutgoingQuorumOrNonVoter(r roachpb.ReplicaDescriptor) bool {
	return isInOutgoingQuorum(r) || isNonVoter(r)
}

func isNonVoter(r roachpb.ReplicaDescriptor) bool {
	return r.Type == roachpb.NON_VOTER
}

func isVoter(r roachpb.ReplicaDescriptor) bool {
	return isInOutgoingQuorum(r) || isInIncomingQuorum(r)
}

func isInIncomingQuorum(r roachpb.ReplicaDescriptor) bool {
	switch r.Type {
	case roachpb.VOTER_FULL, roachpb.VOTER_INCOMING:
		return true
	default:
		return false
	}
}

func isInOutgoingQuorum(r roachpb.ReplicaDescriptor) bool {
	switch r.Type {
	case roachpb.VOTER_FULL, roachpb.VOTER_OUTGOING, roachpb.VOTER_DEMOTING_NON_VOTER, roachpb.VOTER_DEMOTING_LEARNER:
		return true
	default:
		return false
	}
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
