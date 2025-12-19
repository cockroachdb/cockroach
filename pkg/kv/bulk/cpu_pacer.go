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

var cpuPacerRequestDuration = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"bulkio.elastic_cpu_control.request_duration",
	"exeuction time unit to request when pacing CPU requests during various bulk operations",
	50*time.Millisecond,
)

// NewCPUPacer creates a new AC pacer for SST batcher. It will return an empty
// Pacer which noops if db or db.AdmissionPacerFactory is nil.
//
// The setting specifies where waiting in the elastic admission control queue
// is enabled. If disabled, the CPU consumed will be accounted for in
// admission control, but pacing will not wait in admission control.
func NewCPUPacer(ctx context.Context, db *kv.DB, setting *settings.BoolSetting) *admission.Pacer {
	if db == nil || db.AdmissionPacerFactory == nil {
		log.Dev.Infof(ctx, "admission control is not configured to pace this bulk work")
		return nil
	}
	bypassACQueue := !setting.Get(db.SettingsValues())
	tenantID, ok := roachpb.ClientTenantFromContext(ctx)
	if !ok {
		tenantID = roachpb.SystemTenantID
	}
	return db.AdmissionPacerFactory.NewPacer(
		cpuPacerRequestDuration.Get(db.SettingsValues()),
		admission.WorkInfo{
			TenantID:        tenantID,
			Priority:        admissionpb.BulkNormalPri,
			CreateTime:      timeutil.Now().UnixNano(),
			BypassAdmission: bypassACQueue,
		})
}
