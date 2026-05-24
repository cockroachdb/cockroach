// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file implements vCPU consumption tracking for self-hosted clusters.
//
// Background: CockroachDB's usage-based licensing model requires tracking
// vCPU-hours consumed by each node. This package writes hourly audit records
// to system.vcpu_hours_audit for later export via the `cockroach license audit` CLI.
//
// The audit system operates independently of license enforcement - it records
// consumption data even when no license is installed or when licenses expire.

package license

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// defaultVCPUAuditInterval is how often we write vCPU consumption records.
	defaultVCPUAuditInterval = 1 * time.Hour

	// vcpuAuditShutdownTimeout is the maximum time we wait to write the final
	// audit record during shutdown.
	vcpuAuditShutdownTimeout = 5 * time.Second

	// noLicenseID is the sentinel value used when no license is installed.
	// This matches the CLI behavior matrix in the design doc.
	noLicenseID = "no-license"
)

// vcpuAuditData contains the data needed to write a vCPU audit record.
type vcpuAuditData struct {
	// licenseID is the unique identifier for the license.
	// Nil if no license is installed.
	licenseID []byte

	// vcpuCount is the number of vCPUs allocated to this node.
	vcpuCount float64
}

// getVCPUAuditData collects the current vCPU count and license ID.
// This is the data needed to write one audit record.
func (e *Enforcer) getVCPUAuditData(ctx context.Context) vcpuAuditData {
	data := vcpuAuditData{}

	// Get vCPU count from status package (cgroup-aware).
	data.vcpuCount = status.GetVCPUs(ctx)

	// Allow test override for deterministic vCPU counts.
	if tk := e.GetTestingKnobs(); tk != nil && tk.OverrideVCPUCount != nil {
		data.vcpuCount = *tk.OverrideVCPUCount
	}

	licIDRaw := e.currentLicenseID.Load()
	if licIDRaw != nil {
		data.licenseID = licIDRaw.([]byte)
	} else {
		data.licenseID = []byte(noLicenseID)
	}

	return data
}

// getVCPUAuditInterval returns the interval for writing vCPU audit records.
// Can be overridden for testing.
func (e *Enforcer) getVCPUAuditInterval() time.Duration {
	if tk := e.GetTestingKnobs(); tk != nil && tk.OverrideVCPUAuditInterval != nil {
		return *tk.OverrideVCPUAuditInterval
	}
	return defaultVCPUAuditInterval
}

// TestingGetCurrentLicenseID returns the currently cached license ID.
// Used for testing license rotation detection.
func (e *Enforcer) TestingGetCurrentLicenseID() []byte {
	val := e.currentLicenseID.Load()
	if val == nil {
		return nil
	}
	return val.([]byte)
}

// VCPUAuditDataForTest exposes vcpuAuditData for testing.
type VCPUAuditDataForTest struct {
	LicenseID []byte
	VCPUCount float64
}

// GetVCPUAuditDataForTest returns the current vCPU audit data for testing.
func (e *Enforcer) GetVCPUAuditDataForTest(ctx context.Context) VCPUAuditDataForTest {
	data := e.getVCPUAuditData(ctx)
	return VCPUAuditDataForTest{
		LicenseID: data.licenseID,
		VCPUCount: data.vcpuCount,
	}
}

// writeVCPUAuditRecord writes a single vCPU consumption record for the given hour.
// TODO(sadaf-crl): Replace log.Infof with actual SQL INSERT when
// system.vcpu_hours_audit table is available.
func (e *Enforcer) writeVCPUAuditRecord(
	ctx context.Context, nodeID roachpb.NodeID, hourTimestamp time.Time,
) {
	// Collect vCPU data.
	data := e.getVCPUAuditData(ctx)

	// Round timestamp to hour boundary.
	hourTimestamp = hourTimestamp.Truncate(time.Hour)

	// TODO(sadaf-crl): Replace this log statement with SQL INSERT:
	//   INSERT INTO system.vcpu_hours_audit (node_id, license_id, hour_timestamp, num_vcpu)
	//   VALUES ($1, $2, $3, $4)
	log.Dev.Infof(ctx,
		"TODO: write vcpu audit record: node_id=%d license_id=%x hour=%s vcpu_count=%.2f",
		nodeID, data.licenseID, hourTimestamp.UTC().Format(time.RFC3339), data.vcpuCount)
}

// maybeWriteOnLicenseRotation checks if the license ID changed and triggers
// an immediate vCPU audit write to prevent gaps in consumption tracking.
// This is called from RefreshForLicenseChange when the license is updated.
func (e *Enforcer) maybeWriteOnLicenseRotation(ctx context.Context, newLicenseID []byte) {
	// Skip if vCPU audit writer hasn't been started yet.
	if !e.auditStarted.Load() {
		return
	}

	// Get the previous license ID.
	prevLicenseIDRaw := e.currentLicenseID.Load()
	var prevLicenseID []byte
	if prevLicenseIDRaw != nil {
		prevLicenseID = prevLicenseIDRaw.([]byte)
	}

	// Copy the new license ID before storing to prevent mutation.
	var licenseIDCopy []byte
	if newLicenseID != nil {
		licenseIDCopy = append([]byte(nil), newLicenseID...)
	} else {
		licenseIDCopy = []byte(noLicenseID)
	}
	e.currentLicenseID.Store(licenseIDCopy)

	// Check if license ID actually changed.
	if bytes.Equal(prevLicenseID, newLicenseID) {
		// License ID unchanged - no immediate write needed.
		return
	}

	// License rotated - write immediately to close previous license period.
	log.Dev.Infof(ctx, "license rotation detected, writing immediate vCPU audit record")
	e.writeVCPUAuditRecord(ctx, e.nodeID, timeutil.Now())
}

// StartVCPUAuditWriter starts a background goroutine that writes vCPU audit
// records at regular intervals (default: hourly). The writer will:
// - Write one record per hour to system.vcpu_hours_audit
// - Attempt a final write on graceful shutdown (with timeout)
//
// This should be called once during enforcer initialization, after Start().
// Returns an error if called multiple times or with invalid nodeID.
func (e *Enforcer) StartVCPUAuditWriter(
	ctx context.Context, stopper *stop.Stopper, st *cluster.Settings, nodeID roachpb.NodeID,
) error {
	// Validate nodeID.
	if nodeID == 0 {
		return errors.AssertionFailedf("invalid nodeID: %d", nodeID)
	}

	e.nodeID = nodeID

	// Initialize currentLicenseID from the current license to avoid gaps
	// on startup when a license is already installed.
	lic, err := GetLicense(st)
	if err != nil {
		log.Ops.Warningf(ctx, "failed to get initial license for vCPU audit: %v", err)
		e.currentLicenseID.Store([]byte(noLicenseID))
	} else if lic == nil {
		e.currentLicenseID.Store([]byte(noLicenseID))
	} else {
		// Copy the license ID to prevent mutation.
		e.currentLicenseID.Store(append([]byte(nil), lic.LicenseId...))
	}

	// Prevent multiple calls. This CAS acts as a release fence for the
	// nodeID and currentLicenseID writes above.
	if !e.auditStarted.CompareAndSwap(false, true) {
		return errors.New("vCPU audit writer already started")
	}

	return stopper.RunAsyncTask(ctx, "vcpu-audit-writer", func(ctx context.Context) {
		ticker := time.NewTicker(e.getVCPUAuditInterval())
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				e.writeVCPUAuditRecord(ctx, nodeID, timeutil.Now())

			case <-stopper.ShouldQuiesce():
				// Attempt final write with timeout on shutdown.
				_ = timeutil.RunWithTimeout(
					context.Background(), "vcpu-audit-shutdown", vcpuAuditShutdownTimeout,
					func(ctx context.Context) error {
						e.writeVCPUAuditRecord(ctx, nodeID, timeutil.Now())
						return nil
					})
				log.Dev.Infof(ctx, "vcpu audit writer stopped")
				return
			}
		}
	})
}
