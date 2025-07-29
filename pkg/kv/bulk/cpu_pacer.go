// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// CPUPacer wraps an admission.Pacer for use in bulk operations where any errors
// pacing can just be logged (at most once a minute).
type CPUPacer struct {
	pacer    *admission.Pacer
	logEvery *log.EveryN
}

func (p *CPUPacer) Pace(ctx context.Context) {
	if err := p.pacer.Pace(ctx); err != nil {
		if p.logEvery.ShouldLog() {
			// TODO(dt): consider just returning this error/eliminating this wrapper,
			// and making callers bubble up this error if it is only ever a ctx cancel
			// error that the caller should be bubbling up anyway.
			log.Warningf(ctx, "failed to pace SST batcher: %v", err)
		}
	}
}
func (p *CPUPacer) Close() {
	p.pacer.Close()
}

var cpuPacerRequestDuration = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"bulkio.elastic_cpu_control.request_duration",
	"exeuction time unit to request when pacing CPU requests during various bulk operations",
	50*time.Millisecond,
)

// NewCPUPacer creates a new AC pacer for SST batcher. It may return an empty
// Pacer which noops if pacing is disabled or its arguments are nil.
func NewCPUPacer(ctx context.Context, db *kv.DB, setting *settings.BoolSetting) CPUPacer {
	if db == nil || db.AdmissionPacerFactory == nil || !setting.Get(db.SettingsValues()) {
		log.Infof(ctx, "admission control is not configured to pace bulk ingestion")
		return CPUPacer{}
	}
	tenantID, ok := roachpb.ClientTenantFromContext(ctx)
	if !ok {
		tenantID = roachpb.SystemTenantID
	}
	every := log.Every(time.Minute)
	return CPUPacer{pacer: db.AdmissionPacerFactory.NewPacer(
		cpuPacerRequestDuration.Get(db.SettingsValues()),
		admission.WorkInfo{
			TenantID:        tenantID,
			Priority:        admissionpb.BulkNormalPri,
			CreateTime:      timeutil.Now().UnixNano(),
			BypassAdmission: false,
		}),
		logEvery: &every,
	}
}
