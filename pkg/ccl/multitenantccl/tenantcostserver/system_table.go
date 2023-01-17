// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostserver/tenanttokenbucket"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type tenantState struct {
	// Present indicates if the state existed in the table.
	Present bool

	// Time of last update.
	LastUpdate tree.DTimestamp

	// FirstInstance is the smallest active instance ID, or 0 if there are no
	// known active instances.
	FirstInstance base.SQLInstanceID

	// Bucket state, as of LastUpdate.
	Bucket tenanttokenbucket.State

	// Current consumption information.
	Consumption roachpb.TenantConsumption
}

// defaultRefillRate is the default refill rate if it is never configured (via
// the crdb_internal.update_tenant_resource_limits SQL built-in).
const defaultRefillRate = 100

// defaultInitialRUs is the default quantity of RUs available to use immediately
// if it is never configured (via the
// crdb_internal.update_tenant_resource_limits SQL built-in).
// This value is intended to prevent short-running unit tests from being
// throttled.
const defaultInitialRUs = 10 * 1000 * 1000

// maxInstancesCleanup restricts the number of stale instances that are removed
// in a single transaction.
const maxInstancesCleanup = 10

// update accounts for the passing of time since LastUpdate.
// If the tenantState is not initialized (Present=false), it is initialized now.
func (ts *tenantState) update(now time.Time) {
	if !ts.Present {
		*ts = tenantState{
			Present:       true,
			LastUpdate:    tree.DTimestamp{Time: now},
			FirstInstance: 0,
			Bucket: tenanttokenbucket.State{
				RURefillRate: defaultRefillRate,
				RUCurrent:    defaultInitialRUs,
			},
		}
		return
	}
	delta := now.Sub(ts.LastUpdate.Time)
	if delta > 0 {
		// Make sure we never push back LastUpdate, or we'd refill tokens for the
		// same period multiple times.
		ts.Bucket.Update(delta)
		ts.LastUpdate.Time = now
	}
}

type instanceState struct {
	ID base.SQLInstanceID

	// Present indicates if the instance existed in the table.
	Present bool

	// Time of last request from this instance.
	LastUpdate tree.DTimestamp

	// Next active instance ID or 0 if this is the instance with the largest ID.
	NextInstance base.SQLInstanceID

	// Lease uniquely identifies the instance; used to disambiguate different
	// incarnations of the same ID.
	Lease tree.DBytes
	// Seq is a sequence number used to detect duplicate client requests.
	Seq int64
	// Shares term for this instance; see tenanttokenbucket.State.
	Shares float64
}

// sysTableHelper implements the interactions with the system.tenant_usage
// table.
type sysTableHelper struct {
	ctx      context.Context
	tenantID roachpb.TenantID
}

func makeSysTableHelper(ctx context.Context, tenantID roachpb.TenantID) sysTableHelper {
	return sysTableHelper{
		ctx:      ctx,
		tenantID: tenantID,
	}
}

// readTenantState reads the tenant state from the system table.
//
// If the table was not initialized for the tenant, the tenant stats will not be
// Present.
func (h *sysTableHelper) readTenantState(txn isql.Txn) (tenant tenantState, _ error) {
	// We could use a simplified query, but the benefit will be marginal and
	// this is not used in the hot path.
	tenant, _, err := h.readTenantAndInstanceState(txn, 0 /* instanceID */)
	return tenant, err
}

// readTenantAndInstanceState reads the tenant and instance state from the system
// table.
//
// If the table was not initialized for the tenant, the tenant and instance
// state will not be Present.
//
// If the instance is not in the current active set (according to the table),
// the instance state will not be Present.
func (h *sysTableHelper) readTenantAndInstanceState(
	txn isql.Txn, instanceID base.SQLInstanceID,
) (tenant tenantState, instance instanceState, _ error) {
	instance.ID = instanceID
	// Read the two rows for the per-tenant state (instance_id = 0) and the
	// per-instance state.
	rows, err := txn.QueryBufferedEx(
		h.ctx, "tenant-usage-select", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
		  instance_id,               /* 0 */
			next_instance_id,          /* 1 */
			last_update,               /* 2 */
			ru_burst_limit,            /* 3 */
			ru_refill_rate,            /* 4 */
			ru_current,                /* 5 */
			current_share_sum,         /* 6 */
			total_consumption,         /* 7 */
			instance_lease,            /* 8 */
			instance_seq,              /* 9 */
			instance_shares            /* 10 */
		 FROM system.tenant_usage
		 WHERE tenant_id = $1 AND instance_id IN (0, $2)`,
		h.tenantID.ToUint64(),
		int64(instanceID),
	)
	if err != nil {
		return tenantState{}, instanceState{}, err
	}
	for _, r := range rows {
		instanceID := base.SQLInstanceID(tree.MustBeDInt(r[0]))
		if instanceID == 0 {
			// Tenant state.
			tenant.Present = true
			tenant.LastUpdate = tree.MustBeDTimestamp(r[2])
			tenant.FirstInstance = base.SQLInstanceID(tree.MustBeDInt(r[1]))
			tenant.Bucket = tenanttokenbucket.State{
				RUBurstLimit:    float64(tree.MustBeDFloat(r[3])),
				RURefillRate:    float64(tree.MustBeDFloat(r[4])),
				RUCurrent:       float64(tree.MustBeDFloat(r[5])),
				CurrentShareSum: float64(tree.MustBeDFloat(r[6])),
			}
			if consumption := r[7]; consumption != tree.DNull {
				// total_consumption can be NULL because of an upgrade of the
				// tenant_usage table.
				if err := protoutil.Unmarshal(
					[]byte(tree.MustBeDBytes(consumption)), &tenant.Consumption,
				); err != nil {
					return tenantState{}, instanceState{}, err
				}
			}
		} else {
			// Instance state.
			instance.Present = true
			instance.LastUpdate = tree.MustBeDTimestamp(r[2])
			instance.NextInstance = base.SQLInstanceID(tree.MustBeDInt(r[1]))
			instance.Lease = tree.MustBeDBytes(r[8])
			instance.Seq = int64(tree.MustBeDInt(r[9]))
			instance.Shares = float64(tree.MustBeDFloat(r[10]))
		}
	}

	return tenant, instance, nil
}

// updateTenantState writes out an updated tenant state.
func (h *sysTableHelper) updateTenantState(txn isql.Txn, tenant tenantState) error {
	consumption, err := protoutil.Marshal(&tenant.Consumption)
	if err != nil {
		return err
	}
	// Note: it is important that this UPSERT specifies all columns of the
	// table, to allow it to perform "blind" writes.
	_, err = txn.ExecEx(
		h.ctx, "tenant-usage-upsert", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`UPSERT INTO system.tenant_usage(
		  tenant_id,
		  instance_id,
			next_instance_id,
			last_update,
			ru_burst_limit,
			ru_refill_rate,
			ru_current,
			current_share_sum,
			total_consumption,
			instance_lease,
			instance_seq,
			instance_shares
		 ) VALUES ($1, 0, $2, $3, $4, $5, $6, $7, $8, NULL, NULL, NULL)
		 `,
		h.tenantID.ToUint64(),                    // $1
		int64(tenant.FirstInstance),              // $2
		&tenant.LastUpdate,                       // $3
		tenant.Bucket.RUBurstLimit,               // $4
		tenant.Bucket.RURefillRate,               // $5
		tenant.Bucket.RUCurrent,                  // $6
		tenant.Bucket.CurrentShareSum,            // $7
		tree.NewDBytes(tree.DBytes(consumption)), // $8
	)
	return err
}

// updateTenantState writes out updated tenant and instance states.
func (h *sysTableHelper) updateTenantAndInstanceState(
	txn isql.Txn, tenant tenantState, instance instanceState,
) error {
	consumption, err := protoutil.Marshal(&tenant.Consumption)
	if err != nil {
		return err
	}
	// Note: it is important that this UPSERT specifies all columns of the
	// table, to allow it to perform "blind" writes.
	_, err = txn.ExecEx(
		h.ctx, "tenant-usage-insert", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`UPSERT INTO system.tenant_usage(
		  tenant_id,
		  instance_id,
			next_instance_id,
			last_update,
			ru_burst_limit,
			ru_refill_rate,
			ru_current,
			current_share_sum,
			total_consumption,
			instance_lease,
			instance_seq,
			instance_shares
		 ) VALUES
		   ($1, 0,  $2,  $3,  $4,   $5,   $6,   $7,   $8,   NULL, NULL, NULL),
			 ($1, $9, $10, $11, NULL, NULL, NULL, NULL, NULL, $12,  $13,  $14)
		 `,
		h.tenantID.ToUint64(),                    // $1
		int64(tenant.FirstInstance),              // $2
		&tenant.LastUpdate,                       // $3
		tenant.Bucket.RUBurstLimit,               // $4
		tenant.Bucket.RURefillRate,               // $5
		tenant.Bucket.RUCurrent,                  // $6
		tenant.Bucket.CurrentShareSum,            // $7
		tree.NewDBytes(tree.DBytes(consumption)), // $8
		int64(instance.ID),                       // $9
		int64(instance.NextInstance),             // $10
		&instance.LastUpdate,                     // $11
		&instance.Lease,                          // $12
		instance.Seq,                             // $13
		instance.Shares,                          // $14
	)
	return err
}

// accomodateNewInstance is used when we are about to insert a new instance. It
// sets instance.NextInstance and updates the previous instance's
// next_instance_id in the table (or updates tenant.FirstInstance).
//
// Note that this should only happen after a SQL pod starts up (which is
// infrequent). In addition, the SQL pod start-up process is not blocked on
// tenant bucket requests (which happen in the background).
func (h *sysTableHelper) accomodateNewInstance(
	txn isql.Txn, tenant *tenantState, instance *instanceState,
) error {
	if tenant.FirstInstance == 0 || tenant.FirstInstance > instance.ID {
		// The new instance has the lowest ID.
		instance.NextInstance = tenant.FirstInstance
		tenant.FirstInstance = instance.ID
		return nil
	}
	// Find the previous instance.
	row, err := txn.QueryRowEx(
		h.ctx, "find-prev-id", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
		  instance_id,               /* 0 */
			next_instance_id,          /* 1 */
			last_update,               /* 2 */
			instance_lease,            /* 3 */
			instance_seq,              /* 4 */
			instance_shares            /* 5 */
		 FROM system.tenant_usage
		 WHERE tenant_id = $1 AND instance_id > 0 AND instance_id < $2
		 ORDER BY instance_id DESC
		 LIMIT 1`,
		h.tenantID.ToUint64(),
		int64(instance.ID),
	)
	if err != nil {
		return err
	}
	if row == nil {
		return errors.Errorf("could not find row for previous instance")
	}
	prevInstanceID := base.SQLInstanceID(tree.MustBeDInt(row[0]))
	instance.NextInstance = base.SQLInstanceID(tree.MustBeDInt(row[1]))
	prevInstanceLastUpdate := row[2]
	prevInstanceLease := row[3]
	prevInstanceSeq := row[4]
	prevInstanceShares := row[5]

	// Update the previous instance: its next_instance_id is the new instance.
	// TODO(radu): consider coalescing this with updateTenantAndInstanceState to
	// perform a single UPSERT.
	_, err = txn.ExecEx(
		h.ctx, "update-next-id", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		// Update the previous instance's next_instance_id.
		`UPSERT INTO system.tenant_usage(
		  tenant_id,
		  instance_id,
			next_instance_id,
			last_update,
			ru_burst_limit,
			ru_refill_rate,
			ru_current,
			current_share_sum,
			total_consumption,
			instance_lease,
			instance_seq,
			instance_shares
		 ) VALUES ($1, $2, $3, $4, NULL, NULL, NULL, NULL, NULL, $5, $6, $7)
		`,
		h.tenantID.ToUint64(),  // $1
		int64(prevInstanceID),  // $2
		int64(instance.ID),     // $3
		prevInstanceLastUpdate, // $4
		prevInstanceLease,      // $5
		prevInstanceSeq,        // $6
		prevInstanceShares,     // $7
	)
	return err
}

// maybeCleanupStaleInstance checks the last update time of the given instance;
// if it is older than the cutoff time, the instance is removed and the next
// instance ID is returned (this ID is 0 if this is the highest instance ID).
func (h *sysTableHelper) maybeCleanupStaleInstance(
	txn isql.Txn, cutoff time.Time, instanceID base.SQLInstanceID,
) (deleted bool, nextInstance base.SQLInstanceID, _ error) {
	ts := tree.MustMakeDTimestamp(cutoff, time.Microsecond)
	row, err := txn.QueryRowEx(
		h.ctx, "tenant-usage-delete", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.tenant_usage
		 WHERE tenant_id = $1 AND instance_id = $2 AND last_update < $3
		 RETURNING next_instance_id`,
		h.tenantID.ToUint64(),
		int32(instanceID),
		ts,
	)
	if err != nil {
		return false, -1, err
	}
	if row == nil {
		log.VEventf(h.ctx, 1, "tenant %s instance %d not stale", h.tenantID, instanceID)
		return false, -1, nil
	}
	nextInstance = base.SQLInstanceID(tree.MustBeDInt(row[0]))
	log.VEventf(h.ctx, 1, "cleaned up tenant %s instance %d", h.tenantID, instanceID)
	return true, nextInstance, nil
}

// maybeCleanupStaleInstances removes up to maxInstancesCleanup stale instances
// (where the last update time is before the cutoff) with IDs in the range
//
//	[startID, endID).
//
// If endID is -1, then the range is unrestricted [startID, ∞).
//
// Returns the ID of the instance following the deleted instances. This is
// the same with startID if nothing was cleaned up, and it is 0 if we cleaned up
// the last (highest ID) instance.
func (h *sysTableHelper) maybeCleanupStaleInstances(
	txn isql.Txn, cutoff time.Time, startID, endID base.SQLInstanceID,
) (nextInstance base.SQLInstanceID, _ error) {
	log.VEventf(
		h.ctx, 1, "checking stale instances (tenant=%s startID=%d endID=%d)",
		h.tenantID, startID, endID,
	)
	id := startID
	for n := 0; n < maxInstancesCleanup; n++ {
		deleted, nextInstance, err := h.maybeCleanupStaleInstance(txn, cutoff, id)
		if err != nil {
			return -1, err
		}
		if !deleted {
			break
		}
		id = nextInstance
		if id == 0 || (endID != -1 && id >= endID) {
			break
		}
	}
	return id, nil
}

// maybeCheckInvariants checks the invariants for the system table with a random
// probability and only if this is a test build.
func (h *sysTableHelper) maybeCheckInvariants(txn isql.Txn) error {
	if buildutil.CrdbTestBuild && rand.Intn(10) == 0 {
		return h.checkInvariants(txn)
	}
	return nil
}

// checkInvariants reads all rows in the system table for the given tenant and
// checks that the state is consistent.
func (h *sysTableHelper) checkInvariants(txn isql.Txn) error {
	// Read the two rows for the per-tenant state (instance_id = 0) and the
	// per-instance state.
	rows, err := txn.QueryBufferedEx(
		h.ctx, "tenant-usage-select", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
		  instance_id,               /* 0 */
			next_instance_id,          /* 1 */
			last_update,               /* 2 */
			ru_burst_limit,            /* 3 */
			ru_refill_rate,            /* 4 */
			ru_current,                /* 5 */
			current_share_sum,         /* 6 */
			total_consumption,         /* 7 */
			instance_lease,            /* 8 */
			instance_seq,              /* 9 */
			instance_shares            /* 10 */
		 FROM system.tenant_usage
		 WHERE tenant_id = $1
		 ORDER BY instance_id`,
		h.tenantID.ToUint64(),
	)
	if err != nil {
		if h.ctx.Err() == nil {
			log.Warningf(h.ctx, "checkInvariants query failed: %v", err)
		}
		// We don't want to cause a panic for a query error (which is expected
		// during shutdown).
		return nil
	}
	if len(rows) == 0 {
		return nil
	}

	// Get the instance IDs.
	instanceIDs := make([]base.SQLInstanceID, len(rows))
	for i := range rows {
		instanceIDs[i] = base.SQLInstanceID(tree.MustBeDInt(rows[i][0]))
		if i > 0 && instanceIDs[i-1] >= instanceIDs[i] {
			return errors.New("instances out of order")
		}
	}
	if instanceIDs[0] != 0 {
		return errors.New("instance 0 row missing")
	}

	// Check NULL values.
	for i := range rows {
		var nullFirst, nullLast int
		if i == 0 {
			// Row 0 should have NULL per-instance values.
			nullFirst, nullLast = 8, 10
		} else {
			// Other rows should have NULL per-tenant values.
			nullFirst, nullLast = 3, 7
		}
		for j := range rows[i] {
			isNull := (rows[i][j] == tree.DNull)
			expNull := (j >= nullFirst && j <= nullLast)
			if expNull != isNull {
				if !expNull {
					return errors.Errorf("expected NULL column %d", j)
				}
				// We have an exception for total_consumption, which can be NULL because
				// of an upgrade of the tenant_usage table.
				if i != 7 {
					return errors.Errorf("expected non-NULL column %d", j)
				}
			}
		}
	}

	// Verify next_instance_id values.
	for i := range rows {
		expNextInstanceID := base.SQLInstanceID(0)
		if i+1 < len(rows) {
			expNextInstanceID = instanceIDs[i+1]
		}
		nextInstanceID := base.SQLInstanceID(tree.MustBeDInt(rows[i][1]))
		if expNextInstanceID != nextInstanceID {
			return errors.Errorf("expected next instance %d, have %d", expNextInstanceID, nextInstanceID)
		}
	}

	// Verify the shares sum.
	sharesSum := float64(tree.MustBeDFloat(rows[0][6]))
	var expSharesSum float64
	for _, r := range rows[1:] {
		expSharesSum += float64(tree.MustBeDFloat(r[10]))
	}

	a, b := sharesSum, expSharesSum
	if a > b {
		a, b = b, a
	}
	// We use "units of least precision" for similarity: this is the number of
	// representable floating point numbers in-between the two values. This is
	// better than a fixed epsilon because the allowed error is proportional to
	// the magnitude of the numbers. Because the mantissa is in the low bits, we
	// can just use the bit representations as integers.
	const ulpTolerance = 1000
	if math.Float64bits(a)+ulpTolerance <= math.Float64bits(b) {
		return errors.Errorf("expected shares sum %g, have %g", expSharesSum, sharesSum)
	}
	return nil
}

// InspectTenantMetadata returns all the information from the tenant_usage table
// for a given tenant, in a user-readable format (multi-line). Used for testing
// and debugging.
func InspectTenantMetadata(
	ctx context.Context, txn isql.Txn, tenantID roachpb.TenantID, timeFormat string,
) (string, error) {
	h := makeSysTableHelper(ctx, tenantID)
	tenant, err := h.readTenantState(txn)
	if err != nil {
		return "", err
	}
	if !tenant.Present {
		return "empty state", nil
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Bucket state: ru-burst-limit=%g  ru-refill-rate=%g  ru-current=%.12g  current-share-sum=%.12g\n",
		tenant.Bucket.RUBurstLimit,
		tenant.Bucket.RURefillRate,
		tenant.Bucket.RUCurrent,
		tenant.Bucket.CurrentShareSum,
	)
	fmt.Fprintf(&buf, "Consumption: ru=%.12g kvru=%.12g  reads=%d in %d batches (%d bytes)  writes=%d in %d batches (%d bytes)  pod-cpu-usage: %g secs  pgwire-egress=%d bytes  external-egress=%d bytes  external-ingress=%d bytes\n",
		tenant.Consumption.RU,
		tenant.Consumption.KVRU,
		tenant.Consumption.ReadRequests,
		tenant.Consumption.ReadBatches,
		tenant.Consumption.ReadBytes,
		tenant.Consumption.WriteRequests,
		tenant.Consumption.WriteBatches,
		tenant.Consumption.WriteBytes,
		tenant.Consumption.SQLPodsCPUSeconds,
		tenant.Consumption.PGWireEgressBytes,
		tenant.Consumption.ExternalIOEgressBytes,
		tenant.Consumption.ExternalIOIngressBytes,
	)
	fmt.Fprintf(&buf, "Last update: %s\n", tenant.LastUpdate.Time.Format(timeFormat))
	fmt.Fprintf(&buf, "First active instance: %d\n", tenant.FirstInstance)

	rows, err := txn.QueryBufferedEx(
		ctx, "inspect-tenant-state", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
		  instance_id,               /* 0 */
			next_instance_id,          /* 1 */
			last_update,               /* 2 */
			instance_lease,            /* 3 */
			instance_seq,              /* 4 */
			instance_shares            /* 5 */
		 FROM system.tenant_usage
		 WHERE tenant_id = $1 AND instance_id > 0
		 ORDER BY instance_id`,
		tenantID.ToUint64(),
	)
	if err != nil {
		return "", err
	}
	for _, r := range rows {
		fmt.Fprintf(
			&buf, "  Instance %s:  lease=%q  seq=%s  shares=%s  next-instance=%s  last-update=%s\n",
			r[0], tree.MustBeDBytes(r[3]), r[4], r[5], r[1], tree.MustBeDTimestamp(r[2]).Time.Format(timeFormat),
		)
	}
	return buf.String(), nil
}
