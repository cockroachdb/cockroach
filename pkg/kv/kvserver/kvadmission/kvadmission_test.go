// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kvadmission is the integration layer between KV and admission
// control.

package kvadmission

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/stretchr/testify/require"
)

func TestWorkInfoForBatch(t *testing.T) {
	testCases := []struct {
		name                    string
		isAdmin                 bool
		isLeaseInfoRequest      bool
		ah                      kvpb.AdmissionHeader
		requestTenant           int
		rangeTenant             int
		useRangeTenantIDEnabled bool
		bulkOnlyEnabled         bool

		expected admission.WorkInfo
	}{
		{
			name: "other-from-system-tenant-bypasses",
			ah: kvpb.AdmissionHeader{
				Priority: int32(admissionpb.BulkNormalPri), CreateTime: 2, Source: kvpb.AdmissionHeader_OTHER},
			requestTenant: 1, rangeTenant: 4,
			expected: admission.WorkInfo{
				TenantID: roachpb.MustMakeTenantID(1),
				Priority: admissionpb.BulkNormalPri, CreateTime: 2, BypassAdmission: true,
			}},
		{
			name: "other-from-non-system-tenant-does-not-bypass",
			ah: kvpb.AdmissionHeader{
				Priority: int32(admissionpb.BulkNormalPri), CreateTime: 2, Source: kvpb.AdmissionHeader_OTHER},
			requestTenant: 2, rangeTenant: 4,
			expected: admission.WorkInfo{
				TenantID: roachpb.MustMakeTenantID(2),
				Priority: admissionpb.BulkNormalPri, CreateTime: 2, BypassAdmission: false,
			}},
		{
			name:               "is-lease-info-request-bypasses",
			isLeaseInfoRequest: true,
			ah: kvpb.AdmissionHeader{
				Priority: int32(admissionpb.BulkNormalPri), CreateTime: 2, Source: kvpb.AdmissionHeader_FROM_SQL},
			requestTenant: 2, rangeTenant: 4,
			expected: admission.WorkInfo{
				TenantID: roachpb.MustMakeTenantID(2),
				Priority: admissionpb.BulkNormalPri, CreateTime: 2, BypassAdmission: true,
			}},
		{
			name:    "is-admin-from-system-tenant-bypasses",
			isAdmin: true,
			ah: kvpb.AdmissionHeader{
				Priority: int32(admissionpb.LowPri), CreateTime: 5, Source: kvpb.AdmissionHeader_ROOT_KV},
			requestTenant: 1, rangeTenant: 4,
			expected: admission.WorkInfo{
				TenantID: roachpb.MustMakeTenantID(1),
				Priority: admissionpb.LowPri, CreateTime: 5, BypassAdmission: true,
			}},
		{
			name:    "is-admin-from-non-system-tenant-does-not-bypass",
			isAdmin: true,
			ah: kvpb.AdmissionHeader{
				Priority: int32(admissionpb.LowPri), CreateTime: 5, Source: kvpb.AdmissionHeader_ROOT_KV},
			requestTenant: 2, rangeTenant: 4,
			expected: admission.WorkInfo{
				TenantID: roachpb.MustMakeTenantID(2),
				Priority: admissionpb.LowPri, CreateTime: 5, BypassAdmission: false,
			}},
		{
			name: "bulk-only-enabled-from-non-system-tenant-bypasses",
			ah: kvpb.AdmissionHeader{
				Priority: int32(admissionpb.NormalPri), CreateTime: 5, Source: kvpb.AdmissionHeader_FROM_SQL},
			requestTenant: 2, rangeTenant: 4, bulkOnlyEnabled: true,
			expected: admission.WorkInfo{
				TenantID: roachpb.MustMakeTenantID(2),
				Priority: admissionpb.NormalPri, CreateTime: 5, BypassAdmission: true,
			}},
		{
			name: "system-tenant-uses-range-tenant",
			ah: kvpb.AdmissionHeader{
				Priority: int32(admissionpb.NormalPri), CreateTime: 5, Source: kvpb.AdmissionHeader_FROM_SQL},
			requestTenant: 1, rangeTenant: 4, useRangeTenantIDEnabled: true,
			expected: admission.WorkInfo{
				TenantID: roachpb.MustMakeTenantID(4),
				Priority: admissionpb.NormalPri, CreateTime: 5, BypassAdmission: false,
			}},
		{
			name: "non-system-tenant-does-not-use-range-tenant",
			ah: kvpb.AdmissionHeader{
				Priority: int32(admissionpb.NormalPri), CreateTime: 5, Source: kvpb.AdmissionHeader_FROM_SQL},
			requestTenant: 2, rangeTenant: 4, useRangeTenantIDEnabled: true,
			expected: admission.WorkInfo{
				TenantID: roachpb.MustMakeTenantID(2),
				Priority: admissionpb.NormalPri, CreateTime: 5, BypassAdmission: false,
			}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			st := cluster.MakeTestingClusterSettings()
			ctx := context.Background()
			ba := kvpb.BatchRequest{}
			ba.AdmissionHeader = tc.ah
			if tc.isLeaseInfoRequest {
				ba.Add(&kvpb.LeaseInfoRequest{})
			} else if tc.isAdmin {
				ba.Add(&kvpb.AdminSplitRequest{})
			}
			useRangeTenantIDForNonAdminEnabled.Override(ctx, &st.SV, tc.useRangeTenantIDEnabled)
			admission.KVBulkOnlyAdmissionControlEnabled.Override(ctx, &st.SV, tc.bulkOnlyEnabled)
			workInfo := workInfoForBatch(st, roachpb.MustMakeTenantID(uint64(tc.requestTenant)),
				roachpb.MustMakeTenantID(uint64(tc.rangeTenant)), &ba)
			require.Equal(t, tc.expected, workInfo)
		})
	}
}
