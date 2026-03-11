// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/cgroups"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const defaultTelemetryEmissionInterval = time.Hour

// startTelemetryEmitter launches a background goroutine that periodically
// emits license validation telemetry events. It also listens for reactive
// triggers from license changes.
func (e *Enforcer) startTelemetryEmitter(ctx context.Context, stopper *stop.Stopper) {
	if err := stopper.RunAsyncTask(
		ctx, "license-telemetry-emitter", func(ctx context.Context) {
			ticker := time.NewTicker(e.getTelemetryEmissionInterval())
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					e.emitLicenseTelemetry(ctx, "periodic")
				case <-e.telemetryTriggerCh:
					e.emitLicenseTelemetry(ctx, "license-change")
				case <-stopper.ShouldQuiesce():
					return
				}
			}
		},
	); err != nil {
		log.Dev.Warningf(ctx, "failed to start license telemetry emitter: %v", err)
	}
}

// getTelemetryEmissionInterval returns the interval between periodic
// telemetry emissions.
func (e *Enforcer) getTelemetryEmissionInterval() time.Duration {
	if tk := e.GetTestingKnobs(); tk != nil && tk.OverrideTelemetryEmissionInterval != nil {
		return *tk.OverrideTelemetryEmissionInterval
	}
	return defaultTelemetryEmissionInterval
}

// emitLicenseTelemetry builds and emits a LicenseValidationEvent to the
// TELEMETRY log channel, and optionally persists it to the system table
// for air-gapped environments.
func (e *Enforcer) emitLicenseTelemetry(ctx context.Context, reason string) {
	event := &eventpb.LicenseValidationEvent{
		EmissionReason: reason,
	}

	if e.clusterSettings != nil {
		lic, err := GetLicense(e.clusterSettings)
		if err != nil {
			log.Dev.Warningf(ctx, "failed to get license for telemetry: %v", err)
			return
		}
		if lic != nil {
			event.LicenseType = lic.Type.String()
			event.Edition = lic.Edition.String()
			event.OrganizationName = lic.OrganizationName
			event.LicenseID = hex.EncodeToString(lic.LicenseId)
			event.OrganizationID = hex.EncodeToString(lic.OrganizationId)
			event.VCPUEntitled = lic.VcpuEntitled
			event.Environment = lic.Environment.String()
			event.ExpiresAt = lic.ValidUntilUnixSec
			event.IsExpired = timeutil.Now().After(
				timeutil.Unix(lic.ValidUntilUnixSec, 0))
			for _, ao := range lic.AddOns {
				event.AddOns = append(event.AddOns, ao.String())
			}
		} else {
			event.LicenseType = "none"
		}
	} else {
		event.LicenseType = "none"
	}

	// Determine the actual vCPU count from cgroup limits or NumCPU.
	cpuUsage, err := cgroups.GetCgroupCPU()
	if err == nil {
		event.VCPUActual = float32(cpuUsage.CPUShares())
	}

	gpEnd, hasGP := e.GetGracePeriodEndTS()
	if hasGP {
		event.GracePeriodEndAt = gpEnd.Unix()
	}

	log.StructuredEvent(ctx, severity.INFO, event)

	// Persist to system table for air-gapped clusters. This is only
	// available for the system tenant (where e.db is set).
	e.persistTelemetryToSystemTable(ctx, event)
}

// persistTelemetryToSystemTable writes the telemetry event to the
// system.license_telemetry table. This is a best-effort operation;
// failures are logged but do not prevent the event from being emitted
// to the TELEMETRY log channel.
func (e *Enforcer) persistTelemetryToSystemTable(
	ctx context.Context, event *eventpb.LicenseValidationEvent,
) {
	if e.db == nil {
		return
	}
	// TODO(vishal): Implement persistence to system.license_telemetry table
	// once the system table migration is in place. For now, the event is
	// emitted to the TELEMETRY log channel only.
}
