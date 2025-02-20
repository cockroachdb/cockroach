// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcapabilities

import (
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilitiespb"
	"github.com/cockroachdb/errors"
)

// MustGetValueByID will get the value for the capability corresponding to
// the requested ID. If the ID is not valid, this function will panic.
func MustGetValueByID(
	t *tenantcapabilitiespb.TenantCapabilities, id tenantcapabilitiespb.ID,
) Value {
	v, err := GetValueByID(t, id)
	if err != nil {
		panic(err)
	}
	return v
}

// MustGetBoolByID will get the bool value for the capability corresponding to
// the requested ID. If the ID is not valid or the capability is not a bool
// capability, this function will panic.
func MustGetBoolByID(t *tenantcapabilitiespb.TenantCapabilities, id tenantcapabilitiespb.ID) bool {
	return MustGetValueByID(t, id).(BoolValue).Get()
}

// GetValueByID looks up the capability value by ID. It returns an
// error if the ID is not valid.
func GetValueByID(
	t *tenantcapabilitiespb.TenantCapabilities, id tenantcapabilitiespb.ID,
) (Value, error) {
	switch id {
	case tenantcapabilitiespb.CanAdminRelocateRange:
		return (*boolValue)(&t.CanAdminRelocateRange), nil
	case tenantcapabilitiespb.CanAdminScatter:
		return (*invertedBoolValue)(&t.DisableAdminScatter), nil
	case tenantcapabilitiespb.CanAdminSplit:
		return (*invertedBoolValue)(&t.DisableAdminSplit), nil
	case tenantcapabilitiespb.CanAdminUnsplit:
		return (*boolValue)(&t.CanAdminUnsplit), nil
	case tenantcapabilitiespb.CanCheckConsistency:
		return (*boolValue)(&t.CanCheckConsistency), nil
	case tenantcapabilitiespb.CanUseNodelocalStorage:
		return (*boolValue)(&t.CanUseNodelocalStorage), nil
	case tenantcapabilitiespb.CanViewNodeInfo:
		return (*boolValue)(&t.CanViewNodeInfo), nil
	case tenantcapabilitiespb.CanViewTSDBMetrics:
		return (*boolValue)(&t.CanViewTSDBMetrics), nil
	case tenantcapabilitiespb.ExemptFromRateLimiting:
		return (*boolValue)(&t.ExemptFromRateLimiting), nil
	case tenantcapabilitiespb.TenantSpanConfigBounds:
		return &spanConfigBoundsValue{b: &t.SpanConfigBounds}, nil
	case tenantcapabilitiespb.CanDebugProcess:
		return (*boolValue)(&t.CanDebugProcess), nil
	case tenantcapabilitiespb.CanViewAllMetrics:
		return (*boolValue)(&t.CanViewAllMetrics), nil
	case tenantcapabilitiespb.CanPrepareTxns:
		return (*boolValue)(&t.CanPrepareTxns), nil
	default:
		return nil, errors.AssertionFailedf("unknown capability: %q", id.String())
	}
}
