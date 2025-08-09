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

// NewCPUPacer creates a new AC pacer for SST batcher. It may return an empty
// Pacer which noops if pacing is disabled or its arguments are nil.
func NewCPUPacer(ctx context.Context, db *kv.DB, setting *settings.BoolSetting) *admission.Pacer {
	if db == nil || db.AdmissionPacerFactory == nil || !setting.Get(db.SettingsValues()) {
		log.Infof(ctx, "admission control is not configured to pace bulk ingestion")
		return nil
	}
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
			BypassAdmission: false,
		})
}
