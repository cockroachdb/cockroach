// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostserver

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostserver/tenanttokenbucket"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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
	Consumption kvpb.TenantConsumption

	// Rates calculates rate of consumption for tenant metrics that are used
	// by the cost model.
	Rates metricRates
}

// defaultRefillRate is the default refill rate if it is never configured (via
// the crdb_internal.update_tenant_resource_limits SQL built-in). It's high
// enough that if it's not explicitly configured, there should be little or no
// throttling (e.g. for running unit tests).
const defaultRefillRate = 10000

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
				TokenRefillRate: defaultRefillRate,
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
	query := `SELECT
		instance_id,               /* 0 */
		next_instance_id,          /* 1 */
		last_update,               /* 2 */
		ru_burst_limit,            /* 3 */
		ru_refill_rate,            /* 4 */
		ru_current,                /* 5 */
		current_share_sum,         /* 6 */
		total_consumption,         /* 7 */
		current_rates,             /* 8 */
		next_rates,                /* 9 */
		instance_lease,            /* 10 */
		instance_seq               /* 11 */
	 FROM system.tenant_usage
	 WHERE tenant_id = $1 AND instance_id IN (0, $2)`

	unmarshal := func(datum tree.Datum, pb protoutil.Message) error {
		// Column can be null because of a tenant_usage table migration.
		if datum == tree.DNull {
			return nil
		}
		return protoutil.Unmarshal([]byte(tree.MustBeDBytes(datum)), pb)
	}

	rows, err := txn.QueryBufferedEx(
		h.ctx, "tenant-usage-select", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		query,
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
			// NOTE: The current_share_sum column is mapped to the TokenCurrentAvg
			// field. The RU fields are mapped to corresponding Token fields.
			tenant.Present = true
			tenant.LastUpdate = tree.MustBeDTimestamp(r[2])
			tenant.FirstInstance = base.SQLInstanceID(tree.MustBeDInt(r[1]))
			tenant.Bucket = tenanttokenbucket.State{
				TokenBurstLimit: float64(tree.MustBeDFloat(r[3])),
				TokenRefillRate: float64(tree.MustBeDFloat(r[4])),
				TokenCurrent:    float64(tree.MustBeDFloat(r[5])),
				TokenCurrentAvg: float64(tree.MustBeDFloat(r[6])),
			}
			if err = unmarshal(r[7], &tenant.Consumption); err != nil {
				return tenantState{}, instanceState{}, err
			}
			if err = unmarshal(r[8], &tenant.Rates.current); err != nil {
				return tenantState{}, instanceState{}, err
			}
			if err = unmarshal(r[9], &tenant.Rates.next); err != nil {
				return tenantState{}, instanceState{}, err
			}
		} else {
			// Instance state.
			instance.Present = true
			instance.LastUpdate = tree.MustBeDTimestamp(r[2])
			instance.NextInstance = base.SQLInstanceID(tree.MustBeDInt(r[1]))
			instance.Lease = tree.MustBeDBytes(r[10])
			instance.Seq = int64(tree.MustBeDInt(r[11]))
		}
	}

	return tenant, instance, nil
}

// updateTenantState writes out updated tenant and instance states.
func (h *sysTableHelper) updateTenantAndInstanceState(
	txn isql.Txn, tenant *tenantState, instance *instanceState,
) error {
	consumption, err := protoutil.Marshal(&tenant.Consumption)
	if err != nil {
		return err
	}

	currentRates, err := protoutil.Marshal(&tenant.Rates.current)
	if err != nil {
		return err
	}

	nextRates, err := protoutil.Marshal(&tenant.Rates.next)
	if err != nil {
		return err
	}

	query := `UPSERT INTO system.tenant_usage(
		tenant_id,
		instance_id,
		next_instance_id,
		last_update,
		ru_burst_limit,
		ru_refill_rate,
		ru_current,
		current_share_sum,
		total_consumption,
		current_rates,
		next_rates,
		instance_lease,
		instance_seq,
		instance_shares
	) VALUES ($1, 0, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, NULL, NULL)`

	args := []interface{}{
		h.tenantID.ToUint64(),                     // $1
		int64(tenant.FirstInstance),               // $2
		&tenant.LastUpdate,                        // $3
		tenant.Bucket.TokenBurstLimit,             // $4
		tenant.Bucket.TokenRefillRate,             // $5
		tenant.Bucket.TokenCurrent,                // $6
		tenant.Bucket.TokenCurrentAvg,             // $7
		tree.NewDBytes(tree.DBytes(consumption)),  // $8
		tree.NewDBytes(tree.DBytes(currentRates)), // $9
		tree.NewDBytes(tree.DBytes(nextRates)),    // $10
	}

	if instance != nil {
		// Insert an extra row for the instance.
		query += `, ($1, $11, $12, $13, NULL, NULL, NULL, NULL, NULL, NULL, NULL, $14, $15, 0.0)`

		args = append(args,
			int64(instance.ID),           // $11
			int64(instance.NextInstance), // $12
			&instance.LastUpdate,         // $13
			&instance.Lease,              // $14
			instance.Seq,                 // $15
		)
	}

	// Note: it is important that this UPSERT specifies all columns of the
	// table, to allow it to perform "blind" writes.
	// Note: The TokenCurrentAvg field is mapped to the current_share_sum column
	// and the other token fields are mapped to corresponding RU columns.
	_, err = txn.ExecEx(
		h.ctx, "tenant-usage-insert", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		query,
		args...,
	)
	return err
}

// accommodateNewInstance is used when we are about to insert a new instance. It
// sets instance.NextInstance and updates the previous instance's
// next_instance_id in the table (or updates tenant.FirstInstance).
//
// Note that this should only happen after a SQL pod starts up (which is
// infrequent). In addition, the SQL pod start-up process is not blocked on
// tenant bucket requests (which happen in the background).
func (h *sysTableHelper) accommodateNewInstance(
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
			instance_seq               /* 4 */
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
		 ) VALUES ($1, $2, $3, $4, NULL, NULL, NULL, NULL, NULL, $5, $6, 0.0)
		`,
		h.tenantID.ToUint64(),  // $1
		int64(prevInstanceID),  // $2
		int64(instance.ID),     // $3
		prevInstanceLastUpdate, // $4
		prevInstanceLease,      // $5
		prevInstanceSeq,        // $6
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
			instance_seq               /* 9 */
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
			nullFirst, nullLast = 8, 9
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
	fmt.Fprintf(&buf, "Bucket state: token-burst-limit=%g  token-refill-rate=%g  token-current=%.12g  token-current-avg=%.12g\n",
		tenant.Bucket.TokenBurstLimit,
		tenant.Bucket.TokenRefillRate,
		tenant.Bucket.TokenCurrent,
		tenant.Bucket.TokenCurrentAvg,
	)
	fmt.Fprintf(&buf, "Consumption: ru=%.12g kvru=%.12g  reads=%d in %d batches (%d bytes)  writes=%d in %d batches (%d bytes)  pod-cpu-usage: %g secs  pgwire-egress=%d bytes  external-egress=%d bytes  external-ingress=%d bytes  estimated-cpu: %g secs\n",
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
		tenant.Consumption.EstimatedCPUSeconds,
	)
	fmt.Fprintf(&buf, "Rates: write-batches=%.12g,%.12g  estimated-cpu=%.12g,%.12g\n",
		tenant.Rates.current.WriteBatchRate, tenant.Rates.next.WriteBatchRate,
		tenant.Rates.current.EstimatedCPURate, tenant.Rates.next.EstimatedCPURate)
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
			instance_seq               /* 4 */
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
			&buf, "  Instance %s:  lease=%q  seq=%s  next-instance=%s  last-update=%s\n",
			r[0], tree.MustBeDBytes(r[3]), r[4], r[1], tree.MustBeDTimestamp(r[2]).Time.Format(timeFormat),
		)
	}
	return buf.String(), nil
}
