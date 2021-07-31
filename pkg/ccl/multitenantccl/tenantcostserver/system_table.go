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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostserver/tenanttokenbucket"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

type tenantState struct {
	// Present indicates if the state existed in the table.
	Present bool

	// FirstInstance is the smallest active instance ID, or 0 if there are no
	// known active instances.
	FirstInstance base.SQLInstanceID

	Bucket tenanttokenbucket.State

	// Consumption is the current consumption information.
	Consumption roachpb.TokenBucketRequest_Consumption
}

func (ts *tenantState) addConsumption(c roachpb.TokenBucketRequest_Consumption) {
	ts.Consumption.RU += c.RU
	ts.Consumption.ReadRequests += c.ReadRequests
	ts.Consumption.ReadBytes += c.ReadBytes
	ts.Consumption.WriteRequests += c.WriteRequests
	ts.Consumption.WriteBytes += c.WriteBytes
	ts.Consumption.SQLPodCPUSeconds += c.SQLPodCPUSeconds
}

type instanceState struct {
	ID base.SQLInstanceID

	// Present indicates if the instance existed in the table.
	Present bool

	// Next active instance ID or 0 if this is the instance with the largest ID.
	NextInstance base.SQLInstanceID

	// Lease uniquely identifies the instance; used to disambiguate different
	// incarnations of the same ID.
	Lease tree.DBytes
	// Seq is a sequence number used to detect duplicate client requests.
	Seq int64
	// Shares term for this instance; see tenanttokenbucket.State.
	Shares float64
	// Time of last request from this instance.
	LastUpdate tree.DTimestamp
}

// sysTableHelper implements the interactions with the system.tenant_usage
// table.
type sysTableHelper struct {
	ctx      context.Context
	ex       *sql.InternalExecutor
	txn      *kv.Txn
	tenantID roachpb.TenantID
}

func makeSysTableHelper(
	ctx context.Context, ex *sql.InternalExecutor, txn *kv.Txn, tenantID roachpb.TenantID,
) sysTableHelper {
	return sysTableHelper{
		ctx:      ctx,
		ex:       ex,
		txn:      txn,
		tenantID: tenantID,
	}
}

// readTenantState reads the tenant state from the system table.
//
// If the table was not initialized for the tenant, the tenant stats will not be
// Present.
func (h *sysTableHelper) readTenantState() (tenant tenantState, _ error) {
	// We could use a simplified query, but the benefit will be marginal and
	// this is not used in the hot path.
	tenant, _, err := h.readTenantAndInstanceState(0 /* instanceID */)
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
	instanceID base.SQLInstanceID,
) (tenant tenantState, instance instanceState, _ error) {
	instance.ID = instanceID
	// Read the two rows for the per-tenant state (instance_id = 0) and the
	// per-instance state.
	rows, err := h.ex.QueryBufferedEx(
		h.ctx, "tenant-usage-select", h.txn,
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
		  instance_id,               /* 0 */
			next_instance_id,          /* 1 */
			ru_burst_limit,            /* 2 */
			ru_refill_rate,            /* 3 */
			ru_current,                /* 4 */
			current_share_sum,         /* 5 */
			total_ru_usage,            /* 6 */
			total_read_requests,       /* 7 */
			total_read_bytes,          /* 8 */
			total_write_requests,      /* 9 */
			total_write_bytes,         /* 10 */
			total_sql_pod_cpu_seconds, /* 11 */
			instance_lease,            /* 12 */
			instance_seq,              /* 13 */
			instance_shares,           /* 14 */
			instance_last_update       /* 15 */
		 FROM system.tenant_usage
		 WHERE tenant_id = $1 AND instance_id IN (0, $2)`,
		h.tenantID.ToUint64(),
		int64(instanceID),
	)
	if err != nil {
		return tenant, instance, err
	}
	for _, r := range rows {
		instanceID := base.SQLInstanceID(tree.MustBeDInt(r[0]))
		if instanceID == 0 {
			// Tenant state.
			tenant.Present = true
			tenant.FirstInstance = base.SQLInstanceID(tree.MustBeDInt(r[1]))
			tenant.Bucket = tenanttokenbucket.State{
				RUBurstLimit:    float64(tree.MustBeDFloat(r[2])),
				RURefillRate:    float64(tree.MustBeDFloat(r[3])),
				RUCurrent:       float64(tree.MustBeDFloat(r[4])),
				CurrentShareSum: float64(tree.MustBeDFloat(r[5])),
			}
			tenant.Consumption = roachpb.TokenBucketRequest_Consumption{
				RU:               float64(tree.MustBeDFloat(r[6])),
				ReadRequests:     uint64(tree.MustBeDInt(r[7])),
				ReadBytes:        uint64(tree.MustBeDInt(r[8])),
				WriteRequests:    uint64(tree.MustBeDInt(r[9])),
				WriteBytes:       uint64(tree.MustBeDInt(r[10])),
				SQLPodCPUSeconds: float64(tree.MustBeDFloat(r[11])),
			}
		} else {
			// Instance state.
			instance.Present = true
			instance.NextInstance = base.SQLInstanceID(tree.MustBeDInt(r[1]))
			instance.Lease = tree.MustBeDBytes(r[12])
			instance.Seq = int64(tree.MustBeDInt(r[13]))
			instance.Shares = float64(tree.MustBeDFloat(r[14]))
			instance.LastUpdate = tree.MustBeDTimestamp(r[15])
		}
	}

	return tenant, instance, nil
}

// updateTenantState writes out an updated tenant state.
func (h *sysTableHelper) updateTenantState(tenant tenantState) error {
	// Note: it is important that this UPSERT specifies all columns of the
	// table, to allow it to perform "blind" writes.
	_, err := h.ex.ExecEx(
		h.ctx, "tenant-usage-upsert", h.txn,
		sessiondata.NodeUserSessionDataOverride,
		`UPSERT INTO system.tenant_usage(
		  tenant_id,
		  instance_id,
			next_instance_id,
			ru_burst_limit,
			ru_refill_rate,
			ru_current,
			current_share_sum,
			total_ru_usage,
			total_read_requests,
			total_read_bytes,
			total_write_requests,
			total_write_bytes,
			total_sql_pod_cpu_seconds,
			instance_lease,
			instance_seq,
			instance_shares,
			instance_last_update
		 ) VALUES ($1, 0, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULL, NULL, NULL, NULL)
		 `,
		h.tenantID.ToUint64(),
		int64(tenant.FirstInstance),
		tenant.Bucket.RUBurstLimit,
		tenant.Bucket.RURefillRate,
		tenant.Bucket.RUCurrent,
		tenant.Bucket.CurrentShareSum,
		tenant.Consumption.RU,
		tenant.Consumption.ReadRequests,
		tenant.Consumption.ReadBytes,
		tenant.Consumption.WriteRequests,
		tenant.Consumption.WriteBytes,
		tenant.Consumption.SQLPodCPUSeconds,
	)
	return err
}

// updateTenantState writes out updated tenant and instance states.
func (h *sysTableHelper) updateTenantAndInstanceState(
	tenant tenantState, instance instanceState,
) error {
	// Note: it is important that this UPSERT specifies all columns of the
	// table, to allow it to perform "blind" writes.
	_, err := h.ex.ExecEx(
		h.ctx, "tenant-usage-insert", h.txn,
		sessiondata.NodeUserSessionDataOverride,
		`UPSERT INTO system.tenant_usage(
		  tenant_id,
		  instance_id,
			next_instance_id,
			ru_burst_limit,
			ru_refill_rate,
			ru_current,
			current_share_sum,
			total_ru_usage,
			total_read_requests,
			total_read_bytes,
			total_write_requests,
			total_write_bytes,
			total_sql_pod_cpu_seconds,
			instance_lease,
			instance_seq,
			instance_shares,
			instance_last_update
		 ) VALUES
		   ($1, 0, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULL, NULL, NULL, NULL),
			 ($1, $13, $14, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, $15, $16, $17, $18)
		 `,
		h.tenantID.ToUint64(),               // $1
		int64(tenant.FirstInstance),         // $2
		tenant.Bucket.RUBurstLimit,          // $3
		tenant.Bucket.RURefillRate,          // $4
		tenant.Bucket.RUCurrent,             // $5
		tenant.Bucket.CurrentShareSum,       // $6
		tenant.Consumption.RU,               // $7
		tenant.Consumption.ReadRequests,     // $8
		tenant.Consumption.ReadBytes,        // $9
		tenant.Consumption.WriteRequests,    // $10
		tenant.Consumption.WriteBytes,       // $11
		tenant.Consumption.SQLPodCPUSeconds, // $12
		int64(instance.ID),                  // $13
		int64(instance.NextInstance),        // $14
		&instance.Lease,                     // $15
		instance.Seq,                        // $16
		instance.Shares,                     // $17
		&instance.LastUpdate,                // $18
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
func (h *sysTableHelper) accomodateNewInstance(tenant *tenantState, instance *instanceState) error {
	if tenant.FirstInstance == 0 || tenant.FirstInstance > instance.ID {
		// The new instance has the lowest ID.
		instance.NextInstance = tenant.FirstInstance
		tenant.FirstInstance = instance.ID
		return nil
	}
	// Find the previous instance.
	row, err := h.ex.QueryRowEx(
		h.ctx, "find-prev-id", h.txn,
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
		  instance_id,               /* 0 */
			next_instance_id,          /* 1 */
			instance_lease,            /* 2 */
			instance_seq,              /* 3 */
			instance_shares,           /* 4 */
			instance_last_update       /* 5 */
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
	prevInstanceLease := row[2]
	prevInstanceSeq := row[3]
	prevInstanceShares := row[4]
	prevInstanceLastUpdate := row[5]

	// Update the previous instance: its next_instance_id is the new instance.
	// TODO(radu): consider coalescing this with updateTenantAndInstanceState to
	// perform a single UPSERT.
	_, err = h.ex.ExecEx(
		h.ctx, "update-next-id", h.txn,
		sessiondata.NodeUserSessionDataOverride,
		// Update the previous instance's next_instance_id.
		`UPSERT INTO system.tenant_usage(
		  tenant_id,
		  instance_id,
			next_instance_id,
			ru_burst_limit,
			ru_refill_rate,
			ru_current,
			current_share_sum,
			total_ru_usage,
			total_read_requests,
			total_read_bytes,
			total_write_requests,
			total_write_bytes,
			total_sql_pod_cpu_seconds,
			instance_lease,
			instance_seq,
			instance_shares,
			instance_last_update
		 ) VALUES ($1, $2, $3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, $4, $5, $6, $7)
		`,
		h.tenantID.ToUint64(),
		int64(prevInstanceID),
		int64(instance.ID),
		prevInstanceLease,
		prevInstanceSeq,
		prevInstanceShares,
		prevInstanceLastUpdate,
	)
	return err
}

// maybeCheckInvariants checks the invariants for the system table with a random
// probability and only if this is a test build.
func (h *sysTableHelper) maybeCheckInvariants() error {
	if util.CrdbTestBuild && rand.Intn(10) == 0 {
		return h.checkInvariants()
	}
	return nil
}

// checkInvariants reads all rows in the system table for the given tenant and
// checks that the state is consistent.
func (h *sysTableHelper) checkInvariants() error {
	// Read the two rows for the per-tenant state (instance_id = 0) and the
	// per-instance state.
	rows, err := h.ex.QueryBufferedEx(
		h.ctx, "tenant-usage-select", h.txn,
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
		  instance_id,               /* 0 */
			next_instance_id,          /* 1 */
			ru_burst_limit,            /* 2 */
			ru_refill_rate,            /* 3 */
			ru_current,                /* 4 */
			current_share_sum,         /* 5 */
			total_ru_usage,            /* 6 */
			total_read_requests,       /* 7 */
			total_read_bytes,          /* 8 */
			total_write_requests,      /* 9 */
			total_write_bytes,         /* 10 */
			total_sql_pod_cpu_seconds, /* 11 */
			instance_lease,            /* 12 */
			instance_seq,              /* 13 */
			instance_shares,           /* 14 */
			instance_last_update       /* 15 */
		 FROM system.tenant_usage
		 WHERE tenant_id = $1
		 ORDER BY instance_id`,
		h.tenantID.ToUint64(),
	)
	if err != nil {
		return err
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
			nullFirst, nullLast = 12, 15
		} else {
			// Other rows should have NULL per-tenant values.
			nullFirst, nullLast = 2, 11
		}
		for j := range rows[i] {
			isNull := (rows[i][j] == tree.DNull)
			expNull := (j >= nullFirst && j <= nullLast)
			if expNull != isNull {
				if !expNull {
					return errors.Errorf("expected NULL column %d", j)
				}
				return errors.Errorf("expected non-NULL column %d", j)
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
	sharesSum := float64(tree.MustBeDFloat(rows[0][5]))
	var expSharesSum float64
	for _, r := range rows[1:] {
		expSharesSum += float64(tree.MustBeDFloat(r[14]))
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
	ctx context.Context, ex *sql.InternalExecutor, txn *kv.Txn, tenantID roachpb.TenantID,
) (string, error) {
	h := makeSysTableHelper(ctx, ex, txn, tenantID)
	tenant, err := h.readTenantState()
	if err != nil {
		return "", err
	}
	if !tenant.Present {
		return "empty state", nil
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Bucket state: ru-burst-limit=%g  ru-refill-rate=%g  ru-current=%g  current-share-sum=%g\n",
		tenant.Bucket.RUBurstLimit,
		tenant.Bucket.RURefillRate,
		tenant.Bucket.RUCurrent,
		tenant.Bucket.CurrentShareSum,
	)
	fmt.Fprintf(&buf, "Consumption: ru=%g  reads=%d req/%d bytes  writes=%d req/%d bytes  pod-cpu-usage: %g\n",
		tenant.Consumption.RU,
		tenant.Consumption.ReadRequests,
		tenant.Consumption.ReadBytes,
		tenant.Consumption.WriteRequests,
		tenant.Consumption.WriteBytes,
		tenant.Consumption.SQLPodCPUSeconds,
	)
	fmt.Fprintf(&buf, "First active instance: %d\n", tenant.FirstInstance)

	rows, err := ex.QueryBufferedEx(
		ctx, "inspect-tenant-state", txn,
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
		  instance_id,               /* 0 */
			next_instance_id,          /* 1 */
			instance_lease,            /* 2 */
			instance_seq,              /* 3 */
			instance_shares,           /* 4 */
			instance_last_update       /* 5 */
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
			&buf, "  Instance %s:  lease=%s  seq=%s  shares=%s  next-instance=%s\n",
			r[0], r[2], r[3], r[4], r[1],
		)
	}
	return buf.String(), nil
}
